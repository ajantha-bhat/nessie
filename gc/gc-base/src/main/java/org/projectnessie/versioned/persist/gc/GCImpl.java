/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.versioned.persist.gc;

import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the logic to retrieve all contents incl their content keys over all live commits in
 * all named-references.
 */
final class GCImpl implements GC {
  private static final Logger LOGGER = LoggerFactory.getLogger(GCImpl.class);

  private final NessieApiV1 api;
  private final Predicate<EntriesResponse.Entry> contentTypeInclusionPredicate;
  private final Function<Reference, Instant> cutOffTimestamp;
  private final Instant readUntilCommitTimestamp;

  GCImpl(
      NessieApiV1 api,
      Predicate<Type> contentTypeInclusionPredicate,
      Function<Reference, Instant> cutOffTimestamp,
      Instant readUntilCommitTimestamp) {
    this.api = api;
    this.contentTypeInclusionPredicate =
        entry -> contentTypeInclusionPredicate.test(entry.getType());
    this.cutOffTimestamp = cutOffTimestamp;
    this.readUntilCommitTimestamp = readUntilCommitTimestamp;
  }

  @Override
  public <GC_CONTENTS_VALUES extends ContentsValues> GCResult<GC_CONTENTS_VALUES> performGC(
      ContentsValuesCollector<GC_CONTENTS_VALUES> contentsValuesCollector) {
    List<Reference> references = api.getAllReferences().get();

    for (Reference reference : references) {
      Instant cutOffTimestamp = this.cutOffTimestamp.apply(reference);
      walkReference(
          contentsValuesCollector,
          reference,
          commitMeta -> cutOffTimestamp.compareTo(commitMeta.getCommitTime()) < 0);
    }

    return new GCResult<>(contentsValuesCollector.contentsValues);
  }

  private <GC_CONTENTS_VALUES extends ContentsValues> void walkReference(
      ContentsValuesCollector<GC_CONTENTS_VALUES> contentsValuesCollector,
      Reference reference,
      Predicate<CommitMeta> liveCommitPredicate) {
    Consumer<CommitMeta> perCommit =
        commitMeta ->
            handleCommit(
                contentsValuesCollector,
                reference,
                commitMeta,
                liveCommitPredicate.test(commitMeta));

    try (Stream<CommitMeta> commits =
        StreamingUtil.getCommitLogStream(
            api, reference.getName(), null, null, null, OptionalInt.empty())) {
      Spliterator<CommitMeta> src = commits.spliterator();

      // Use a Spliterator to limit the processed commits to the "live" commits - i.e. stop at the
      // first "non-live" commit.
      new AbstractSpliterator<CommitMeta>(src.estimateSize(), 0) {
        private boolean more = true;
        private boolean any = false;

        @Override
        public boolean tryAdvance(Consumer<? super CommitMeta> action) {
          if (!more) {
            return false;
          }
          more =
              src.tryAdvance(
                  commitMeta -> {
                    if (readUntilCommitTimestamp.compareTo(commitMeta.getCommitTime()) > 0) {
                      more = false;
                    }
                    // Process the commit if either the commit is considered live (i.e. the
                    // liveCommitPredicate yields 'true') or at least for the very first commit
                    // (aka the reference's HEAD).
                    if (more || !any) {
                      action.accept(commitMeta);
                      any = true;
                    }
                  });
          return more;
        }
      }.forEachRemaining(perCommit);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof NessieNotFoundException) {
        LOGGER.info("Reference {} to retrieve commits for no longer exists", reference);
      } else {
        throw e;
      }
    } catch (NessieNotFoundException e) {
      LOGGER.info("Reference {} to retrieve commits for no longer exists", reference);
    }
  }

  private <GC_CONTENTS_VALUES extends ContentsValues> void handleCommit(
      ContentsValuesCollector<GC_CONTENTS_VALUES> contentsValuesCollector,
      Reference reference,
      CommitMeta commitMeta,
      boolean isLive) {
    try {
      Reference commitReference = referenceWithHash(reference, commitMeta);

      // TODO this is not ideal, because the (loop outside this method) iterates over all commits
      //  and this method first retrieves all keys on that commit and then retrieves all values
      //  on the same commit. However, it is good enough for a first iteration of the Nessie GC.

      List<ContentsKey> allKeys =
          api.getEntries().reference(commitReference).get().getEntries().stream()
              .filter(contentTypeInclusionPredicate)
              .map(EntriesResponse.Entry::getName)
              .collect(Collectors.toList());
      api.getContents()
          .reference(commitReference)
          .keys(allKeys)
          .get()
          .forEach(
              (contentsKey, contents) ->
                  handleValue(
                      contentsValuesCollector, commitReference, contentsKey, contents, isLive));
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private <GC_CONTENTS_VALUES extends ContentsValues> void handleValue(
      ContentsValuesCollector<GC_CONTENTS_VALUES> contentsValuesCollector,
      Reference reference,
      ContentsKey contentsKey,
      Contents contents,
      boolean isLive) {
    // TODO reduce to TRACE or remove log at all
    LOGGER.info(
        "{} value contents-id {} at commit {} in {} with key {}: {}",
        isLive ? "Live" : "Non-live",
        contents.getId(),
        reference.getHash(),
        reference.getName(),
        contentsKey,
        contents);
    contentsValuesCollector.gotValue(contents, reference, contentsKey, isLive);
  }

  private Reference referenceWithHash(Reference reference, CommitMeta commitMeta) {
    if (reference instanceof Branch) {
      reference = Branch.of(reference.getName(), commitMeta.getHash());
    } else if (reference instanceof Tag) {
      reference = Tag.of(reference.getName(), commitMeta.getHash());
    } else {
      throw new IllegalArgumentException(
          String.format("Reference %s is neither a branch nor a tag", reference));
    }
    return reference;
  }
}
