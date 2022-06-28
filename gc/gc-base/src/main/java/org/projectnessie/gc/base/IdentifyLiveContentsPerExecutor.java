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

import com.google.common.hash.BloomFilter;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.api.params.FetchOption;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the methods that execute in spark executor for computing live content bloom filter in
 * {@link GCImpl#identifyExpiredContents(SparkSession)}.
 */
public class IdentifyLiveContentsPerExecutor implements Serializable {

  private final GCParams gcParams;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IdentifyLiveContentsPerExecutor.class);

  public IdentifyLiveContentsPerExecutor(GCParams gcParams) {
    this.gcParams = gcParams;
  }

  protected Function<String, LiveContentsResult> computeLiveContentsFunc(
      long bloomFilterSize, Map<String, Instant> droppedRefTimeMap) {
    return reference ->
        computeLiveContents(
            GCImpl.getCutoffTimeForRef(gcParams, reference, droppedRefTimeMap),
            reference,
            droppedRefTimeMap.get(reference),
            bloomFilterSize);
  }

  /**
   * Traverse the live commits stream till an entry is seen for each live content key and at least
   * reached expired commits.
   *
   * @param foundAllLiveCommitHeadsBeforeCutoffTime condition to stop traversing
   * @param commits stream of {@link LogResponse.LogEntry}
   * @param commitHandler consumer of {@link LogResponse.LogEntry}
   * @return last visited commit hash. It is the commit hash when all the live contents heads at the
   *     cutoff time are found.
   */
  static String traverseLiveCommits(
      MutableBoolean foundAllLiveCommitHeadsBeforeCutoffTime,
      Stream<LogResponse.LogEntry> commits,
      Consumer<LogResponse.LogEntry> commitHandler) {
    AtomicReference<String> lastVisitedHash = new AtomicReference<>();
    Spliterator<LogResponse.LogEntry> src = commits.spliterator();
    // Use a Spliterator to limit the processed commits to the "live" commits - i.e. stop traversing
    // the expired commits once an entry is seen for each live content key.
    new Spliterators.AbstractSpliterator<LogResponse.LogEntry>(src.estimateSize(), 0) {
      private boolean more = true;

      @Override
      public boolean tryAdvance(Consumer<? super LogResponse.LogEntry> action) {
        if (!more) {
          return false;
        }
        more =
            src.tryAdvance(
                logEntry -> {
                  // traverse until all the live commit heads are found for each live keys
                  // at the time of cutoff time.
                  if (foundAllLiveCommitHeadsBeforeCutoffTime.isFalse()) {
                    action.accept(logEntry);
                  }
                  lastVisitedHash.set(logEntry.getCommitMeta().getHash());
                });
        more = more && foundAllLiveCommitHeadsBeforeCutoffTime.isFalse();
        return more;
      }
    }.forEachRemaining(commitHandler);

    return lastVisitedHash.get();
  }

  private LiveContentsResult computeLiveContents(
      Instant cutOffTimestamp, String reference, Instant droppedRefTime, long bloomFilterSize) {
    NessieApiV1 api = GCUtil.getApi(gcParams.getNessieClientConfigs());
    TaskContext.get()
        .addTaskCompletionListener(
            context -> {
              LOGGER.info("Closing the nessie api for compute live contents task");
              api.close();
            });
    LOGGER.debug("Computing live contents for {}", reference);
    Reference ref = GCUtil.deserializeReference(reference);
    boolean isRefDroppedAfterCutoffTimeStamp =
        droppedRefTime == null || droppedRefTime.compareTo(cutOffTimestamp) >= 0;
    if (!isRefDroppedAfterCutoffTimeStamp) {
      // reference is dropped before cutoff time.
      // All the contents for all the keys are expired.
      return ImmutableLiveContentsResult.builder()
          .lastLiveCommitHash(ref.getHash())
          .referenceName(ref.getName())
          .hashOnReference(ref.getHash())
          .bloomFilter(null)
          .build();
    }
    Predicate<CommitMeta> liveCommitPredicate =
        commitMeta ->
            // If the commit time is newer than (think: greater than or equal to) cutoff-time,
            // then commit is live.
            commitMeta.getCommitTime().compareTo(cutOffTimestamp) >= 0;

    ImmutableGCStateParamsPerTask gcStateParamsPerTask =
        ImmutableGCStateParamsPerTask.builder()
            .api(api)
            .reference(ref)
            .liveCommitPredicate(liveCommitPredicate)
            .bloomFilterSize(bloomFilterSize)
            .build();

    return walkLiveCommitsInReference(gcStateParamsPerTask);
  }

  private LiveContentsResult walkLiveCommitsInReference(GCStateParamsPerTask gcStateParamsPerTask) {
    try (Stream<LogResponse.LogEntry> commits =
        StreamingUtil.getCommitLogStream(
            gcStateParamsPerTask.getApi(),
            builder ->
                builder
                    .hashOnRef(gcStateParamsPerTask.getReference().getHash())
                    .refName(Detached.REF_NAME)
                    .fetch(FetchOption.ALL),
            OptionalInt.empty())) {
      BloomFilter<Content> bloomFilter =
          BloomFilter.create(
              new ContentFunnel(),
              gcStateParamsPerTask.getBloomFilterSize(),
              gcParams.getBloomFilterFpp());
      Set<ContentKey> liveContentKeys = new HashSet<>();
      MutableBoolean foundAllLiveCommitHeadsBeforeCutoffTime = new MutableBoolean(false);
      // commit handler for the spliterator
      Consumer<LogResponse.LogEntry> commitHandler =
          logEntry ->
              handleLiveCommit(
                  gcStateParamsPerTask,
                  logEntry,
                  bloomFilter,
                  foundAllLiveCommitHeadsBeforeCutoffTime,
                  liveContentKeys);
      // traverse commits using the spliterator
      String lastVisitedHash =
          traverseLiveCommits(foundAllLiveCommitHeadsBeforeCutoffTime, commits, commitHandler);
      if (lastVisitedHash == null) {
        // can happen when the branch has no commits.
        lastVisitedHash = gcStateParamsPerTask.getReference().getHash();
      }
      LOGGER.debug(
          "For the reference {} last traversed commit {}",
          gcStateParamsPerTask.getReference().getName(),
          lastVisitedHash);
      return ImmutableLiveContentsResult.builder()
          .lastLiveCommitHash(lastVisitedHash)
          .referenceName(gcStateParamsPerTask.getReference().getName())
          .hashOnReference(gcStateParamsPerTask.getReference().getHash())
          .bloomFilter(bloomFilter)
          .build();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private void handleLiveCommit(
      GCStateParamsPerTask gcStateParamsPerTask,
      LogResponse.LogEntry logEntry,
      BloomFilter<Content> bloomFilter,
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
                  bloomFilter.put(((Operation.Put) operation).getContent());
                }
              });
    }
  }
}
