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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.ImmutableDelete;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the logic to retrieve all content incl their content keys over all commits in all
 * named-references.
 */
final class GCImpl implements GC {
  private static final Logger LOGGER = LoggerFactory.getLogger(GCImpl.class);

  private final NessieApiV1 api;
  private final Function<Reference, Instant> cutOffTimestamp;

  GCImpl(NessieApiV1 api, Function<Reference, Instant> cutOffTimestamp) {
    this.api = api;
    this.cutOffTimestamp = cutOffTimestamp;
  }

  @Override
  public <GC_CONTENT_VALUES extends ExpiredContentValues> GCResult<GC_CONTENT_VALUES> performGC(
      ContentValuesCollector<GC_CONTENT_VALUES> contentValuesCollector) {
    List<Reference> references = api.getAllReferences().get().getReferences();

    for (Reference reference : references) {
      Instant cutOffTimestamp = this.cutOffTimestamp.apply(reference);
      walkReference(
          contentValuesCollector,
          reference,
          commitMeta ->
              // If the commit time is before (think: less than, older) cutoff-time stamp, then
              // commit is expired.
              Objects.requireNonNull(commitMeta.getCommitTime()).compareTo(cutOffTimestamp) < 0);
    }

    return new GCResult<>(contentValuesCollector.contentValues);
  }

  private <GC_CONTENT_VALUES extends ExpiredContentValues> void walkReference(
      ContentValuesCollector<GC_CONTENT_VALUES> contentValuesCollector,
      Reference reference,
      Predicate<CommitMeta> expiredCommitPredicate) {
    try (Stream<LogResponse.LogEntry> commits =
        StreamingUtil.getCommitLogStream(
            api, reference.getName(), null, null, null, OptionalInt.empty(), true)) {

      Set<ContentKey> deletedKeys = new HashSet<>();
      AtomicBoolean commitHead = new AtomicBoolean(true);
      commits.forEach(
          (logEntry) -> {
            boolean isExpired;
            if (commitHead.get()) {
              // If the commit is head commit, then it should be kept live if the table is reachable
              isExpired = false;
              commitHead.set(false);
            } else {
              // check expiry based on cutoff time
              isExpired = expiredCommitPredicate.test(logEntry.getCommitMeta());
            }

            if (logEntry.getOperations() != null) {
              logEntry
                  .getOperations()
                  .forEach(
                      operation -> {
                        if (operation instanceof ImmutableDelete) {
                          deletedKeys.add(operation.getKey());
                        }
                      });
            }
            handleCommit(
                contentValuesCollector,
                reference,
                logEntry.getCommitMeta(),
                deletedKeys,
                isExpired);
          });
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

  private <GC_CONTENT_VALUES extends ExpiredContentValues> void handleCommit(
      ContentValuesCollector<GC_CONTENT_VALUES> contentValuesCollector,
      Reference reference,
      CommitMeta commitMeta,
      Set<ContentKey> deletedKeys,
      boolean isExpired) {
    try {
      Reference commitReference = referenceWithHash(reference, commitMeta);
      List<ContentKey> allKeys =
          api.getEntries().reference(commitReference).get().getEntries().stream()
              .map(EntriesResponse.Entry::getName)
              .collect(Collectors.toList());
      api.getContent()
          .reference(commitReference)
          .keys(allKeys)
          .get()
          .forEach(
              (contentKey, content) -> {
                boolean isContentExpired = isExpired;
                if (deletedKeys.contains(contentKey)) {
                  // table is dropped. So, expire all the contents for this ContentKey independent
                  // of cutOffTimeStamp.
                  isContentExpired = true;
                }
                handleValue(
                    contentValuesCollector, commitReference, contentKey, content, isContentExpired);
              });
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private <GC_CONTENT_VALUES extends ExpiredContentValues> void handleValue(
      ContentValuesCollector<GC_CONTENT_VALUES> contentValuesCollector,
      Reference reference,
      ContentKey contentKey,
      Content content,
      boolean isExpired) {
    LOGGER.info(
        "{} value content-id {} at commit {} in {} with key {}: {}",
        isExpired ? "Expired" : "Live",
        content.getId(),
        reference.getHash(),
        reference.getName(),
        contentKey,
        content);
    contentValuesCollector.gotValue(content, reference, contentKey, isExpired);
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
