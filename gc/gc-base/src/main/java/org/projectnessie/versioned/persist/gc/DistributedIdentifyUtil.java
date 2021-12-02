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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedIdentifyUtil implements Serializable {

  private static final long serialVersionUID = -5412153338307931076L;
  private final SparkSession session;
  private final Map<String, String> config;
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedIdentifyUtil.class);

  public DistributedIdentifyUtil(SparkSession session, Map<String, String> config) {
    this.session = session;
    this.config = config;
  }

  /**
   * Builds the client builder; default({@link HttpClientBuilder}) or custom, based on the
   * configuration provided.
   *
   * @param configuration map of client builder configurations.
   * @return {@link NessieApiV1} object.
   */
  public static NessieApiV1 getApi(Map<String, String> configuration) {
    String clientBuilderClassName = configuration.get("nessie.client-builder-impl");
    if (clientBuilderClassName == null) {
      // Use default HttpClientBuilder
      return HttpClientBuilder.builder().fromConfig(configuration::get).build(NessieApiV1.class);
    }
    // Use custom client builder
    NessieClientBuilder builder;
    try {
      builder =
          (NessieClientBuilder)
              Class.forName(clientBuilderClassName).getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("No custom client builder class found for '%s' ", clientBuilderClassName));
    } catch (InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Could not initialize '%s': ", clientBuilderClassName), e);
    }
    return (NessieApiV1) builder.fromConfig(configuration::get).build(NessieApiV1.class);
  }

  /**
   * Compute the bloom filter per content id by walking all the live references in a distributed way
   * using spark.
   *
   * @param references list of live references to walk.
   * @param cutOffTimestampPerRef map of cutoff timestamp per reference.
   * @param defaultCutOffTimestamp default cutoff timestamp to be used.
   * @return map of {@link ContentBloomFilter} per content-id.
   */
  public Map<String, ContentBloomFilter> getLiveContentsBloomFilters(
      List<Reference> references,
      Map<String, Instant> cutOffTimestampPerRef,
      Instant defaultCutOffTimestamp) {
    List<Map<String, ContentBloomFilter>> bloomFilterMaps =
        new JavaSparkContext(session.sparkContext())
            .parallelize(references, getTaskCount(references))
            .map(
                reference ->
                    computeLiveContents(
                        config, cutOffTimestampPerRef, defaultCutOffTimestamp, reference))
            .collect();
    return mergeLiveContentResults(bloomFilterMaps);
  }

  /**
   * Gets the live and expired contents per content id by walking all the live and dead references
   * in a distributed way using spark and checking the contents against the live bloom filter
   * results.
   *
   * @param liveContentsBloomFilterMap live contents bloom filter per content id.
   * @param references list of all the references to walk (live and dead)
   * @return {@link IdentifiedResult} object.
   */
  public IdentifiedResult getIdentifiedResults(
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap, List<Reference> references) {

    List<IdentifiedResult> results =
        new JavaSparkContext(session.sparkContext())
            .parallelize(references, getTaskCount(references))
            .map(reference -> computeExpiredContents(config, liveContentsBloomFilterMap, reference))
            .collect();
    // TODO: write each task output directly into the spark table.
    //  Each executor task can return an iterator of spark ROW
    //  and it can be written directly to the spark table.
    //  Instead of accumulating/merging each task output to a single in-memory map.
    IdentifiedResult identifiedResult = new IdentifiedResult();
    results.forEach(result -> identifiedResult.addNewContentValues(result.getContentValues()));
    return identifiedResult;
  }

  protected Map<String, ContentBloomFilter> computeLiveContents(
      Map<String, String> config,
      Map<String, Instant> cutOffTimestampPerRef,
      Instant defaultCutOffTimestamp,
      Reference reference) {
    try (NessieApiV1 api = getApi(config)) {
      Instant cutOffTimestamp =
          cutOffTimestampPerRef.getOrDefault(reference.getName(), defaultCutOffTimestamp);
      return walkLiveReference(
          api,
          reference,
          commitMeta ->
              // If the commit time is newer than (think: greater than or equal to) cutoff-time,
              // then
              // commit is live.
              Objects.requireNonNull(commitMeta.getCommitTime()).compareTo(cutOffTimestamp) >= 0);
    }
  }

  protected @NotNull IdentifiedResult computeExpiredContents(
      Map<String, String> config,
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap,
      Reference reference) {
    try (NessieApiV1 api = getApi(config)) {
      return walkAllReference(api, reference, liveContentsBloomFilterMap);
    }
  }

  private int getTaskCount(List<Reference> references) {
    int taskCount =
        Integer.parseInt(
            config.getOrDefault("nessie.gc.spark.task.count", String.valueOf(references.size())));
    if (taskCount <= 0) {
      LOGGER.warn("Invalid nessie.gc.spark.task.count configuration: {} using default", taskCount);
      taskCount = references.size();
    }
    return taskCount;
  }

  private Map<String, ContentBloomFilter> mergeLiveContentResults(
      List<Map<String, ContentBloomFilter>> bloomFilterMaps) {
    Map<String, ContentBloomFilter> output = new HashMap<>();
    bloomFilterMaps.forEach(
        map ->
            map.forEach(
                (k, v) -> {
                  if (output.containsKey(k)) {
                    output.get(k).merge(v);
                  } else {
                    output.put(k, v);
                  }
                }));
    return output;
  }

  private Map<String, ContentBloomFilter> walkLiveReference(
      NessieApiV1 api, Reference reference, Predicate<CommitMeta> liveCommitPredicate) {
    Map<String, ContentBloomFilter> bloomFilterMap = new HashMap<>();
    Set<ContentKey> deletedKeys = new HashSet<>();
    try (Stream<LogResponse.LogEntry> commits =
        StreamingUtil.getCommitLogStream(
            api, reference.getName(), null, null, null, OptionalInt.empty(), true)) {
      Spliterator<LogResponse.LogEntry> src = commits.spliterator();
      // Use a Spliterator to limit the processed commits to the "live" commits - i.e. stop at the
      // first "non-live" commit.
      new Spliterators.AbstractSpliterator<LogResponse.LogEntry>(src.estimateSize(), 0) {
        private boolean more = true;
        private boolean refHead = true;

        @Override
        public boolean tryAdvance(Consumer<? super LogResponse.LogEntry> action) {
          if (!more) {
            return false;
          }
          more =
              src.tryAdvance(
                  logEntry -> {
                    if (!liveCommitPredicate.test(logEntry.getCommitMeta())) {
                      // found first expired commit
                      more = false;
                    }
                    // Process the commit if either the commit is considered live (i.e. the
                    // liveCommitPredicate yields 'true') or at least for the very first commit
                    // (aka the reference's HEAD).
                    if (more || refHead) {
                      action.accept(logEntry);
                      refHead = false;
                    }
                  });
          return more;
        }
      }.forEachRemaining(logEntry -> handleLiveCommit(logEntry, deletedKeys, bloomFilterMap));
    } catch (NessieNotFoundException e) {
      LOGGER.info("Reference {} to retrieve commits for no longer exists", reference);
    }
    return bloomFilterMap;
  }

  private IdentifiedResult walkAllReference(
      NessieApiV1 api,
      Reference reference,
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap) {
    IdentifiedResult result = new IdentifiedResult();
    try (Stream<LogResponse.LogEntry> commits =
        StreamingUtil.getCommitLogStream(
            api, "DETACHED", reference.getHash(), null, null, OptionalInt.empty(), true)) {
      commits.forEach(logEntry -> handleCommit(logEntry, liveContentsBloomFilterMap, result));
    } catch (NessieNotFoundException e) {
      LOGGER.info("Reference {} to retrieve commits no longer exists", reference);
    }
    return result;
  }

  private void handleLiveCommit(
      LogResponse.LogEntry logEntry,
      Set<ContentKey> deletedKeys,
      Map<String, ContentBloomFilter> bloomFilterMap) {
    if (logEntry.getOperations() != null) {
      logEntry
          .getOperations()
          .forEach(
              operation -> {
                if (operation instanceof Operation.Delete) {
                  // collect the deleted keys
                  deletedKeys.add(operation.getKey());
                } else if (operation instanceof Operation.Put) {
                  Content content = ((Operation.Put) operation).getContent();
                  boolean isDropped = deletedKeys.contains(operation.getKey());
                  if (!isDropped || bloomFilterMap.containsKey(content.getId())) {
                    // If the content key is Not dropped, it is live commit.
                    // If the content key is dropped and previously traversed commits have this
                    // content id means
                    // it is not a drop table, it is re-name table operation. So, consider it as
                    // live.
                    bloomFilterMap
                        .computeIfAbsent(content.getId(), k -> new ContentBloomFilter(config))
                        .put(content);
                  }
                }
              });
    }
  }

  private void handleCommit(
      LogResponse.LogEntry logEntry,
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap,
      IdentifiedResult result) {
    if (logEntry.getOperations() != null) {
      logEntry
          .getOperations()
          .forEach(
              operation -> {
                if (operation instanceof Operation.Put) {
                  Content content = ((Operation.Put) operation).getContent();

                  ContentBloomFilter bloomFilter = liveContentsBloomFilterMap.get(content.getId());
                  // when no live bloom filter exist for this content id, all the contents are
                  // definitely expired.
                  // when bloom filter says it is definitely not live, add it to the expired list.
                  boolean isExpired = bloomFilter == null || !bloomFilter.mightContain(content);
                  result.addContent(content, operation.getKey(), isExpired);
                }
              });
    }
  }
}
