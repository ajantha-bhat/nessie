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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Contains the methods that executes in spark executor for {@link
 * GCImpl#identifyExpiredContents(SparkSession)}.
 */
public class IdentifyContentsPerExecutor implements Serializable {

  private final GCParams gcParams;
  private static final Logger LOGGER = LoggerFactory.getLogger(IdentifyContentsPerExecutor.class);

  public IdentifyContentsPerExecutor(GCParams gcParams) {
    this.gcParams = gcParams;
  }

  protected Function<String, Map<String, ContentBloomFilter>> computeLiveContentsFunc(
      long bloomFilterSize, Map<String, Instant> droppedRefTimeMap) {
    return reference ->
        computeLiveContents(
            getCutoffTimeForRef(reference, droppedRefTimeMap),
            reference,
            droppedRefTimeMap.get(reference),
            bloomFilterSize);
  }

  protected SerializableFunction1<scala.collection.Iterator<String>, scala.collection.Iterator<Row>>
      getExpiredContentRowsFunc(
          Map<String, ContentBloomFilter> liveContentsBloomFilterMap,
          String runId,
          Timestamp startedAt,
          Map<String, Instant> droppedRefTimeMap) {
    return result ->
        getExpiredContentRows(
            result, liveContentsBloomFilterMap, runId, startedAt, droppedRefTimeMap);
  }

  // --- Methods for computing live content bloom filter ----------
  private Map<String, ContentBloomFilter> computeLiveContents(
      Instant cutOffTimestamp, String reference, Instant droppedRefTime, long bloomFilterSize) {
    NessieApiV1 api = GCUtil.getApi(gcParams.getNessieClientConfigs());
    TaskContext.get()
        .addTaskCompletionListener(
            context -> {
              LOGGER.info("Closing the nessie api for compute live contents task");
              api.close();
            });
    boolean isRefDroppedAfterCutoffTimeStamp =
        droppedRefTime == null || droppedRefTime.compareTo(cutOffTimestamp) >= 0;
    if (!isRefDroppedAfterCutoffTimeStamp) {
      // reference is dropped before cutoff time.
      // All the contents for all the keys are expired.
      return new HashMap<>();
    }
    Predicate<CommitMeta> liveCommitPredicate =
        commitMeta ->
            // If the commit time is newer than (think: greater than or equal to) cutoff-time,
            // then commit is live.
            commitMeta.getCommitTime().compareTo(cutOffTimestamp) >= 0;

    ImmutableGCStateParamsPerTask gcStateParamsPerTask =
        ImmutableGCStateParamsPerTask.builder()
            .api(api)
            .reference(GCUtil.deserializeReference(reference))
            .liveCommitPredicate(liveCommitPredicate)
            .bloomFilterSize(bloomFilterSize)
            .build();

    return walkLiveCommitsInReference(gcStateParamsPerTask);
  }

  private Map<String, ContentBloomFilter> walkLiveCommitsInReference(
      GCStateParamsPerTask gcStateParamsPerTask) {
    Map<String, ContentBloomFilter> bloomFilterMap = new HashMap<>();
    Set<ContentKey> liveContentKeys = new HashSet<>();
    try (Stream<LogResponse.LogEntry> commits =
        StreamingUtil.getCommitLogStream(
            gcStateParamsPerTask.getApi(),
            builder ->
                builder
                    .hashOnRef(gcStateParamsPerTask.getReference().getHash())
                    .refName(Detached.REF_NAME)
                    .fetch(FetchOption.ALL),
            OptionalInt.empty())) {
      MutableBoolean foundAllLiveCommitHeadsBeforeCutoffTime = new MutableBoolean(false);
      // commit handler for the spliterator
      Consumer<LogResponse.LogEntry> commitHandler =
          logEntry ->
              handleLiveCommit(
                  gcStateParamsPerTask,
                  logEntry,
                  bloomFilterMap,
                  foundAllLiveCommitHeadsBeforeCutoffTime,
                  liveContentKeys);
      // traverse commits using the spliterator
      GCUtil.traverseLiveCommits(foundAllLiveCommitHeadsBeforeCutoffTime, commits, commitHandler);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    return bloomFilterMap;
  }

  private void handleLiveCommit(
      GCStateParamsPerTask gcStateParamsPerTask,
      LogResponse.LogEntry logEntry,
      Map<String, ContentBloomFilter> bloomFilterMap,
      MutableBoolean foundAllLiveCommitHeadsBeforeCutoffTime,
      Set<ContentKey> liveContentKeys) {
    if (logEntry.getOperations() != null) {
      boolean isExpired =
          !gcStateParamsPerTask.getLiveCommitPredicate().test(logEntry.getCommitMeta());
      if (isExpired && liveContentKeys.isEmpty()) {
        // get live content keys for this reference at hash
        // as it is the first expired commit. Time travel is supported till this state.
        try {
          gcStateParamsPerTask
              .getApi()
              .getEntries()
              .refName(Detached.REF_NAME)
              .hashOnRef(logEntry.getCommitMeta().getHash())
              .get()
              .getEntries()
              .forEach(entries -> liveContentKeys.add(entries.getName()));

          if (liveContentKeys.isEmpty()) {
            // no contents are live at the time of cutoff time
            foundAllLiveCommitHeadsBeforeCutoffTime.setTrue();
            return;
          }
        } catch (NessieNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
      logEntry.getOperations().stream()
          .filter(Operation.Put.class::isInstance)
          .forEach(
              operation -> {
                boolean addContent;
                if (liveContentKeys.contains(operation.getKey())) {
                  // commit head of this key
                  addContent = true;
                  liveContentKeys.remove(operation.getKey());
                  if (liveContentKeys.isEmpty()) {
                    // found all the live commit heads before cutoff time.
                    foundAllLiveCommitHeadsBeforeCutoffTime.setTrue();
                  }
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
                                  gcStateParamsPerTask.getBloomFilterSize(),
                                  gcParams.getBloomFilterFpp()))
                      .put(content);
                }
              });
    }
  }

  private Instant getCutoffTimeForRef(String reference, Map<String, Instant> droppedRefTimeMap) {
    if (droppedRefTimeMap.containsKey(reference)
        && gcParams.getDeadReferenceCutOffTimeStamp() != null) {
      // if the reference is dropped and deadReferenceCutOffTimeStamp is configured, use it.
      return gcParams.getDeadReferenceCutOffTimeStamp();
    }
    return gcParams.getCutOffTimestampPerRef() == null
        ? gcParams.getDefaultCutOffTimestamp()
        : gcParams
            .getCutOffTimestampPerRef()
            .getOrDefault(
                GCUtil.deserializeReference(reference).getName(),
                gcParams.getDefaultCutOffTimestamp());
  }

  // --- Methods for computing expired content rows ----------
  /**
   * convert scala Iterator of reference into scala Iterator of {@link Row}.
   *
   * <p>Each reference can produce one or more rows.
   */
  private scala.collection.Iterator<Row> getExpiredContentRows(
      scala.collection.Iterator<String> references,
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap,
      String runId,
      Timestamp startedAt,
      Map<String, Instant> droppedRefTimeMap) {
    NessieApiV1 api = GCUtil.getApi(gcParams.getNessieClientConfigs());
    TaskContext.get()
        .addTaskCompletionListener(
            context -> {
              LOGGER.info("Closing the nessie api for compute expired contents task");
              api.close();
            });
    return references.flatMap(
        reference ->
            JavaConverters.asScalaIterator(
                    walkAllCommitsInReference(
                        api,
                        GCUtil.deserializeReference(reference),
                        liveContentsBloomFilterMap,
                        runId,
                        startedAt,
                        getCutoffTimeForRef(reference, droppedRefTimeMap)))
                .toTraversable());
  }

  private Iterator<Row> walkAllCommitsInReference(
      NessieApiV1 api,
      Reference reference,
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap,
      String runId,
      Timestamp startedAt,
      Instant cutoffTime) {
    // To filter only the commits that are expired based on cutoff time.
    // cutoff time also acts as a commit protection time for ongoing
    // or new commits created after step-1 of identify gc.
    Predicate<LogResponse.LogEntry> cutoffTimePredicate =
        logEntry -> logEntry.getCommitMeta().getCommitTime().compareTo(cutoffTime) < 0;
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
    Predicate<Content> expiredContentPredicate =
        content ->
            (liveContentsBloomFilterMap.get(content.getId()) == null
                || !liveContentsBloomFilterMap.get(content.getId()).mightContain(content));
    Predicate<Content> validSnapshotPredicate =
        content ->
            (content instanceof IcebergTable && ((IcebergTable) content).getSnapshotId() != -1
                || content instanceof IcebergView && ((IcebergView) content).getVersionId() != -1);
    try {
      AtomicReference<String> commitHash = new AtomicReference<>();
      Iterator<Content> iterator =
          StreamingUtil.getCommitLogStream(
                  api,
                  builder ->
                      builder
                          .hashOnRef(reference.getHash())
                          .refName(Detached.REF_NAME)
                          .fetch(FetchOption.ALL),
                  OptionalInt.empty())
              .filter(cutoffTimePredicate)
              .map(
                  entry -> {
                    commitHash.set(entry.getCommitMeta().getHash());
                    return entry.getOperations();
                  })
              .flatMap(operations -> operations.stream().filter(Operation.Put.class::isInstance))
              .map(Operation.Put.class::cast)
              .map(Operation.Put::getContent)
              .filter(
                  content ->
                      (content.getType() == Content.Type.ICEBERG_TABLE
                          || content.getType() == Content.Type.ICEBERG_VIEW))
              .filter(expiredContentPredicate)
              .filter(validSnapshotPredicate)
              .iterator();

      return new Iterator<Row>() {
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public Row next() {
          return fillRow(reference, iterator.next(), runId, startedAt, commitHash.get());
        }
      };
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static Row fillRow(
      Reference reference, Content content, String runId, Timestamp startedAt, String commitHash) {
    return IdentifiedResultsRepo.createContentRow(
        content, runId, startedAt, getSnapshotId(content), reference, commitHash);
  }

  private static long getSnapshotId(Content content) {
    long snapshotId;
    switch (content.getType()) {
      case ICEBERG_VIEW:
        snapshotId = ((IcebergView) content).getVersionId();
        break;
      case ICEBERG_TABLE:
      default:
        snapshotId = ((IcebergTable) content).getSnapshotId();
    }
    return snapshotId;
  }
}
