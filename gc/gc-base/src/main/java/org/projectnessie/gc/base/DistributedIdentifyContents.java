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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Identify the expired and live contents in a distributed way using the spark and bloom filter by
 * walking all the references (both dead and live).
 */
public class DistributedIdentifyContents {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedIdentifyContents.class);

  private final SparkSession session;
  private final GCParams gcParams;

  public DistributedIdentifyContents(SparkSession session, GCParams gcParams) {
    this.session = session;
    this.gcParams = gcParams;
  }

  /**
   * Compute the bloom filter by walking all the live references in a distributed way using spark.
   *
   * @param references list of all the references (JSON serialized)
   * @param bloomFilterSize size of bloom filter to be used
   * @param droppedRefTimeMap map of dropped time for reference@hash (JSON serialized)
   * @param runId current run id of the gc
   * @param startedAt gc start time
   * @return {@link ContentBloomFilter} object.
   */
  public ContentBloomFilter getLiveContentsBloomFilter(
      List<String> references,
      long bloomFilterSize,
      Map<String, Instant> droppedRefTimeMap,
      String runId,
      Timestamp startedAt) {
    IdentifyContentsPerExecutor executor = new IdentifyContentsPerExecutor(gcParams);
    List<LiveContentsResult> liveContentsResults =
        new JavaSparkContext(session.sparkContext())
            .parallelize(references, getPartitionsCount(gcParams, references))
            .map(executor.computeLiveContentsFunc(bloomFilterSize, droppedRefTimeMap))
            .collect();
    List<ContentBloomFilter> filters = new ArrayList<>();
    List<Row> checkPointRows = new ArrayList<>();
    liveContentsResults.forEach(
        result -> {
          if (result.getBloomFilter() != null) {
            filters.add(result.getBloomFilter());
          }
          checkPointRows.add(IdentifiedResultsRepo.createCheckPointRow(runId, startedAt, result));
        });
    IdentifiedResultsRepo identifiedResultsRepo =
        new IdentifiedResultsRepo(
            session,
            gcParams.getNessieCatalogName(),
            gcParams.getOutputBranchName(),
            gcParams.getOutputTableIdentifier());
    identifiedResultsRepo.writeToOutputTable(checkPointRows);
    return mergeLiveContentResults(filters, gcParams.getBloomFilterFpp());
  }

  /**
   * Gets the expired contents per content id by walking all the live and dead references in a
   * distributed way using spark and checking the contents against the live bloom filter results.
   *
   * @param liveContentsBloomFilter live contents bloom filter.
   * @param references list of all the references (JSON serialized) to walk (live and dead)
   * @param droppedRefTimeMap map of dropped time for reference@hash (JSON serialized)
   * @param runId current run id of the GC
   * @param startedAt GC start time
   * @return current run id of the completed gc task
   */
  public String identifyExpiredContents(
      ContentBloomFilter liveContentsBloomFilter,
      List<String> references,
      Map<String, Instant> droppedRefTimeMap,
      String runId,
      Timestamp startedAt) {
    IdentifiedResultsRepo identifiedResultsRepo =
        new IdentifiedResultsRepo(
            session,
            gcParams.getNessieCatalogName(),
            gcParams.getOutputBranchName(),
            gcParams.getOutputTableIdentifier());
    Map<String, String> commitCheckPoints = identifiedResultsRepo.collectLatestCommitCheckPoint();

    IdentifyContentsPerExecutor executor = new IdentifyContentsPerExecutor(gcParams);
    Dataset<Row> rowDataset =
        session
            .createDataset(references, Encoders.STRING())
            .mapPartitions(
                executor.getExpiredContentRowsFunc(
                    liveContentsBloomFilter,
                    runId,
                    startedAt,
                    droppedRefTimeMap,
                    commitCheckPoints),
                RowEncoder.apply(IdentifiedResultsRepo.getSchema()));
    identifiedResultsRepo.writeToOutputTable(rowDataset);
    return runId;
  }

  private static int getPartitionsCount(GCParams gcParams, List<String> references) {
    return gcParams.getSparkPartitionsCount() == null
        ? references.size()
        : gcParams.getSparkPartitionsCount();
  }

  private static ContentBloomFilter mergeLiveContentResults(
      List<ContentBloomFilter> bloomFilters, double bloomFilterFpp) {
    if (bloomFilters.isEmpty()) {
      return null;
    }
    ContentBloomFilter output = bloomFilters.get(0);
    bloomFilters.subList(1, bloomFilters.size()).forEach(output::merge);

    // Since we merged bloom filters log in case their quality deteriorated
    if (output.getExpectedFpp() > bloomFilterFpp) {
      LOGGER.info(
          "Fpp of merged ContentBloomFilter is {}", String.format("%.3f", output.getExpectedFpp()));
    }
    return output;
  }
}
