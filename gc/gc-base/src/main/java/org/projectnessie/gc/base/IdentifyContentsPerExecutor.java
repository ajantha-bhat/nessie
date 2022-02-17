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
package org.projectnessie.gc.base;

import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

/**
 * Contains the methods that executes in spark executor for {@link
 * GCImpl#identifyExpiredContents(SparkSession)}.
 */
public class IdentifyContentsPerExecutor implements Serializable {

  private final GCParams gcParams;

  public IdentifyContentsPerExecutor(GCParams gcParams) {
    this.gcParams = gcParams;
  }

  protected Function<Reference, Map<String, ContentBloomFilter>> computeLiveContentsFunc(
      long totalCommitsInDefaultReference, Map<Reference, Instant> droppedRefTimeMap) {
    return reference ->
        computeLiveContents(
            getCutoffTimeForRef(reference, droppedRefTimeMap),
            reference,
            droppedRefTimeMap.get(reference),
            totalCommitsInDefaultReference);
  }

  protected Function<Reference, IdentifiedResult> computeExpiredContentsFunc(
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap) {
    return reference -> computeExpiredContents(liveContentsBloomFilterMap, reference);
  }

  private Map<String, ContentBloomFilter> computeLiveContents(
      Instant cutOffTimestamp,
      Reference reference,
      Instant droppedRefTime,
      long totalCommitsInDefaultReference) {
    try (NessieApiV1 api = GCUtil.getApi(gcParams.getNessieClientConfigs())) {
      boolean isRefDroppedAfterCutoffTimeStamp =
          droppedRefTime == null || droppedRefTime.compareTo(cutOffTimestamp) >= 0;
      Predicate<CommitMeta> liveCommitPredicate =
          commitMeta ->
              // If the commit time is newer than (think: greater than or equal to) cutoff-time,
              // then commit is live.
              commitMeta.getCommitTime().compareTo(cutOffTimestamp) >= 0;

      ImmutableGCStateParamsPerTask gcStateParamsPerTask =
          ImmutableGCStateParamsPerTask.builder()
              .api(api)
              .reference(reference)
              .isRefDroppedAfterCutoffTimeStamp(isRefDroppedAfterCutoffTimeStamp)
              .liveCommitPredicate(liveCommitPredicate)
              .totalCommitsInDefaultReference(totalCommitsInDefaultReference)
              .build();

      return walkLiveCommitsInReference(gcStateParamsPerTask);
    }
  }

  private IdentifiedResult computeExpiredContents(
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap, Reference reference) {
    try (NessieApiV1 api = GCUtil.getApi(gcParams.getNessieClientConfigs())) {
      return walkAllCommitsInReference(api, reference, liveContentsBloomFilterMap);
    }
  }

  private Map<String, ContentBloomFilter> walkLiveCommitsInReference(
      GCStateParamsPerTask gcStateParamsPerTask) {
    Map<String, ContentBloomFilter> bloomFilterMap = new HashMap<>();
    Set<ContentKey> liveContentKeys = new HashSet<>();
    try (Stream<LogResponse.LogEntry> commits =
        StreamingUtil.getCommitLogStream(
            gcStateParamsPerTask.getApi(),
            Detached.REF_NAME,
            gcStateParamsPerTask.getReference().getHash(),
            null,
            null,
            OptionalInt.empty(),
            true)) {
      MutableBoolean isLiveContentsKeyAdded = new MutableBoolean(false);
      // commit handler for the spliterator
      Consumer<LogResponse.LogEntry> commitHandler =
          logEntry ->
              handleLiveCommit(
                  gcStateParamsPerTask,
                  logEntry,
                  bloomFilterMap,
                  isLiveContentsKeyAdded,
                  liveContentKeys);
      // traverse commits using the spliterator
      GCUtil.traverseLiveCommits(
          gcStateParamsPerTask.getLiveCommitPredicate(),
          isLiveContentsKeyAdded,
          liveContentKeys,
          commits,
          commitHandler);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    return bloomFilterMap;
  }

  private IdentifiedResult walkAllCommitsInReference(
      NessieApiV1 api,
      Reference reference,
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap) {
    IdentifiedResult result = new IdentifiedResult();
    int commitProtectionTimeInHours = gcParams.getCommitProtectionTime();
    Instant commitProtectionTime =
        (commitProtectionTimeInHours == 0)
            ? null
            : Instant.now().minus(commitProtectionTimeInHours, ChronoUnit.HOURS);
    try (Stream<LogResponse.LogEntry> commits =
        StreamingUtil.getCommitLogStream(
            api, Detached.REF_NAME, reference.getHash(), null, null, OptionalInt.empty(), true)) {
      commits.forEach(
          logEntry -> {
            // Between the bloom filter creation and this step,
            // there can be some more commits in the backend.
            // Checking them against bloom filter will give false results.
            // Hence, protect those commits using commitProtectionTime.
            boolean isUnprotectedCommit =
                (commitProtectionTime == null)
                    || (logEntry.getCommitMeta().getCommitTime().compareTo(commitProtectionTime)
                        < 0);
            if (isUnprotectedCommit) {
              // this commit can be tested for expiry.
              handleCommitForExpiredContents(
                  reference, logEntry, liveContentsBloomFilterMap, result);
            }
          });
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  private void handleLiveCommit(
      GCStateParamsPerTask gcStateParamsPerTask,
      LogResponse.LogEntry logEntry,
      Map<String, ContentBloomFilter> bloomFilterMap,
      MutableBoolean isLiveContentsKeyAdded,
      Set<ContentKey> liveContentKeys) {
    if (logEntry.getOperations() != null) {
      boolean isExpired =
          !gcStateParamsPerTask.getLiveCommitPredicate().test(logEntry.getCommitMeta());
      if (gcStateParamsPerTask.isRefDroppedAfterCutoffTimeStamp()
          && isLiveContentsKeyAdded.isFalse()
          && isExpired) {
        // get live content keys for this reference at hash
        // as it is the first expired commit. Time travel is supported till this state.
        // Also, If the isRefDroppedAfterCutoffTimeStamp is false,
        // no need to add live content keys as everything is expired.
        try {
          gcStateParamsPerTask
              .getApi()
              .getEntries()
              .refName(Detached.REF_NAME)
              .hashOnRef(logEntry.getCommitMeta().getHash())
              .get()
              .getEntries()
              .forEach(entries -> liveContentKeys.add(entries.getName()));
          isLiveContentsKeyAdded.setTrue();
        } catch (NessieNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
      logEntry.getOperations().stream()
          .filter(operation -> operation instanceof Operation.Put)
          .forEach(
              operation -> {
                boolean addContent;
                if (isLiveContentsKeyAdded.isTrue()
                    && liveContentKeys.contains(operation.getKey())) {
                  // commit head of this key
                  addContent = true;
                  liveContentKeys.remove(operation.getKey());
                } else {
                  addContent = !isExpired;
                }
                if (addContent) {
                  Content content = ((Operation.Put) operation).getContent();
                  bloomFilterMap
                      .computeIfAbsent(
                          content.getId(),
                          k ->
                              new ContentBloomFilter(
                                  gcParams,
                                  gcStateParamsPerTask.getTotalCommitsInDefaultReference()))
                      .put(content);
                }
              });
    }
  }

  private static void handleCommitForExpiredContents(
      Reference reference,
      LogResponse.LogEntry logEntry,
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap,
      IdentifiedResult result) {
    if (logEntry.getOperations() != null) {
      logEntry.getOperations().stream()
          .filter(operation -> operation instanceof Operation.Put)
          .forEach(
              operation -> {
                Content content = ((Operation.Put) operation).getContent();
                ContentBloomFilter bloomFilter = liveContentsBloomFilterMap.get(content.getId());
                // when no live bloom filter exist for this content id, all the contents are
                // definitely expired.

                // Checking against live contents bloom filter can result
                //    no  --> content is considered expired (filter cannot say 'no' for live
                // contents).
                //    may be  -->  content is considered live (filter can say 'maybe' for expired
                // contents).
                // Worst case few expired contents will be considered live due to bloom filter
                // fpp.
                // But live contents never be considered as expired.
                if (bloomFilter == null || !bloomFilter.mightContain(content)) {
                  result.addContent(reference.getName(), content);
                }
              });
    }
  }

  private Instant getCutoffTimeForRef(
      Reference reference, Map<Reference, Instant> droppedRefTimeMap) {
    if (droppedRefTimeMap.containsKey(reference)
        && gcParams.getDeadReferenceCutOffTimeStamp() != null) {
      // if the reference is dropped and deadReferenceCutOffTimeStamp is configured, use it.
      return gcParams.getDeadReferenceCutOffTimeStamp();
    }
    return gcParams.getCutOffTimestampPerRef() == null
        ? gcParams.getDefaultCutOffTimestamp()
        : gcParams
            .getCutOffTimestampPerRef()
            .getOrDefault(reference.getName(), gcParams.getDefaultCutOffTimestamp());
  }
}
