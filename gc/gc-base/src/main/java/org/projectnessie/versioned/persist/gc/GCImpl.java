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
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the logic to retrieve all content incl their content keys over all live commits in
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
  public <GC_CONTENT_VALUES extends ContentValues> GCResult<GC_CONTENT_VALUES> performGC(
      ContentValuesCollector<GC_CONTENT_VALUES> contentValuesCollector) {
    List<Reference> references = api.getAllReferences().get().getReferences();

    for (Reference reference : references) {
      Instant cutOffTimestamp = this.cutOffTimestamp.apply(reference);
      walkReference(
          contentValuesCollector,
          reference,
          commitMeta -> cutOffTimestamp.compareTo(commitMeta.getCommitTime()) < 0);
    }

    return new GCResult<>(contentValuesCollector.contentValues);
  }

  private <GC_CONTENT_VALUES extends ContentValues> void walkReference(
      ContentValuesCollector<GC_CONTENT_VALUES> contentValuesCollector,
      Reference reference,
      Predicate<CommitMeta> liveCommitPredicate) {
    Consumer<CommitMeta> perCommit =
        commitMeta ->
            handleCommit(
                contentValuesCollector,
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

  private <GC_CONTENT_VALUES extends ContentValues> void handleCommit(
      ContentValuesCollector<GC_CONTENT_VALUES> contentValuesCollector,
      Reference reference,
      CommitMeta commitMeta,
      boolean isLive) {
    try {
      Reference commitReference = referenceWithHash(reference, commitMeta);

      // TODO this is not ideal, because the (loop outside this method) iterates over all commits
      //  and this method first retrieves all keys on that commit and then retrieves all values
      //  on the same commit. However, it is good enough for a first iteration of the Nessie GC.

      List<ContentKey> allKeys =
          api.getEntries().reference(commitReference).get().getEntries().stream()
              .filter(contentTypeInclusionPredicate)
              .map(EntriesResponse.Entry::getName)
              .collect(Collectors.toList());
      api.getContent()
          .reference(commitReference)
          .keys(allKeys)
          .get()
          .forEach(
              (contentKey, content) ->
                  handleValue(
                      contentValuesCollector, commitReference, contentKey, content, isLive));
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private <GC_CONTENT_VALUES extends ContentValues> void handleValue(
      ContentValuesCollector<GC_CONTENT_VALUES> contentValuesCollector,
      Reference reference,
      ContentKey contentKey,
      Content content,
      boolean isLive) {
    // TODO reduce to TRACE or remove log at all
    LOGGER.info(
        "{} value content-id {} at commit {} in {} with key {}: {}",
        isLive ? "Live" : "Non-live",
        content.getId(),
        reference.getHash(),
        reference.getName(),
        contentKey,
        content);
    contentValuesCollector.gotValue(content, reference, contentKey, isLive);
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
