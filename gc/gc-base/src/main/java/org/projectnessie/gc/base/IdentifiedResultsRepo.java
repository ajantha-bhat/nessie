/*
 * Copyright (C) 2022 Dremio
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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;
import org.projectnessie.model.Content;
import org.projectnessie.model.ImmutableTableReference;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DDL + DML functionality for the "IdentifiedResult" table. */
public final class IdentifiedResultsRepo {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdentifiedResultsRepo.class);

  private static final String COL_GC_RUN_START = "gcRunStart";
  private static final String COL_GC_RUN_ID = "gcRunId";
  private static final String COL_ROW_TYPE = "rowType";
  private static final String COL_CONTENT_ID = "contentId";
  private static final String COL_CONTENT_TYPE = "contentType";
  private static final String COL_SNAPSHOT_ID = "snapshotId";
  private static final String COL_REFERENCE_NAME = "referenceName";
  private static final String COL_HASH_ON_REFERENCE = "hashOnReference";
  private static final String COL_COMMIT_HASH = "commitHash";
  private static final String COL_METADATA_LOCATION = "metadataLocation";
  private static final String COL_IS_EXPIRED = "isExpired";

  public enum RowType {
    CONTENT_OUTPUT,
    CHECKPOINT,
    CHECKPOINT_MARKER
  }

  private static final Schema icebergSchema =
      new Schema(
          Types.StructType.of(
                  // GC run start timestamp.
                  required(1, COL_GC_RUN_START, Types.TimestampType.withZone()),
                  // GC run-ID.
                  required(2, COL_GC_RUN_ID, Types.StringType.get()),
                  // row type can be CONTENT_OUTPUT, CHECKPOINT or CHECKPOINT_MARKER.
                  required(3, COL_ROW_TYPE, Types.StringType.get()),
                  // Nessie Content.id
                  optional(4, COL_CONTENT_ID, Types.StringType.get()),
                  // Nessie Content.type
                  optional(5, COL_CONTENT_TYPE, Types.StringType.get()),
                  // Iceberg Table/View Content's snapshot/version id.
                  optional(6, COL_SNAPSHOT_ID, Types.LongType.get()),
                  // Name of the reference via which the contentID was collected
                  optional(7, COL_REFERENCE_NAME, Types.StringType.get()),
                  // Hash of the reference via which the contentID was collected
                  optional(8, COL_HASH_ON_REFERENCE, Types.StringType.get()),
                  // commit hash which is containing this content
                  optional(9, COL_COMMIT_HASH, Types.StringType.get()),
                  // metadata location of this content
                  optional(10, COL_METADATA_LOCATION, Types.StringType.get()),
                  // to indicate whether this content is expired or live
                  optional(11, COL_IS_EXPIRED, Types.BooleanType.get()))
              .fields());

  private static final StructType schema = SparkSchemaUtil.convert(icebergSchema);

  private final SparkSession sparkSession;
  private final String catalogAndTableWithRefName;

  public IdentifiedResultsRepo(
      SparkSession sparkSession, String catalog, String gcBranchName, String gcTableIdentifier) {
    this.sparkSession = sparkSession;
    this.catalogAndTableWithRefName = withRefName(catalog, gcTableIdentifier, gcBranchName);
    createTableIfAbsent(
        sparkSession, catalog, TableIdentifier.parse(gcTableIdentifier), gcBranchName);
  }

  public static StructType getSchema() {
    return schema;
  }

  /**
   * Collect the expired contents for the given run id as spark dataset.
   *
   * @param runId run id of completed identify task.
   * @return spark dataset of row where each row is having the expired contents per content id per
   *     reference.
   */
  public Dataset<Row> collectExpiredContentsAsDataSet(String runId) {
    return getRowDataset(runId, true);
  }

  /**
   * Collect the live contents for the given run id as spark dataset.
   *
   * @param runId run id of completed identify task.
   * @return spark dataset of row where each row is having the live contents per content id per
   *     reference.
   */
  public Dataset<Row> collectLiveContentsAsDataSet(String runId) {
    return getRowDataset(runId, false);
  }

  public Optional<String> getLatestCompletedRunID() {
    // collect row for the last written run id
    // Example query:
    // SELECT gcRunId FROM nessie.db2.`identified_results@someGcBranch` WHERE gcRunStart =
    //    (SELECT MAX(gcRunStart) FROM nessie.db2.`identified_results@someGcBranch`
    //    WHERE rowType = "CONTENT_OUTPUT") LIMIT 1
    List<Row> rows =
        sql(
                "SELECT %s FROM %s WHERE %s = (SELECT MAX(%s) FROM %s WHERE %s = '%s') LIMIT 1",
                COL_GC_RUN_ID,
                //
                catalogAndTableWithRefName,
                //
                COL_GC_RUN_START,
                COL_GC_RUN_START,
                catalogAndTableWithRefName,
                //
                COL_ROW_TYPE,
                RowType.CONTENT_OUTPUT.name())
            .collectAsList();
    return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0).getString(0));
  }

  public Row createCheckPointMarkerRow(String runId) {
    return RowFactory.create(
        getGcStartTime(runId),
        runId,
        RowType.CHECKPOINT_MARKER.name(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public void writeToOutputTable(List<Row> rows) {
    writeToOutputTable(sparkSession.createDataFrame(rows, schema));
  }

  void writeToOutputTable(Dataset<Row> rowDataset) {
    try {
      // write content rows to the output table
      rowDataset.writeTo(catalogAndTableWithRefName).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          "Problem while writing output rows to the table: " + catalogAndTableWithRefName, e);
    }
  }

  static Row createContentRow(
      Content content,
      String runId,
      Timestamp startedAt,
      long snapshotId,
      Reference ref,
      String commitHash,
      String metadataLocation,
      boolean isExpired) {
    return RowFactory.create(
        startedAt,
        runId,
        RowType.CONTENT_OUTPUT.name(),
        content.getId(),
        content.getType().name(),
        snapshotId,
        ref.getName(),
        ref.getHash(),
        commitHash,
        metadataLocation,
        isExpired);
  }

  static Row createCheckPointRow(String runId, Timestamp startedAt, LiveContentsResult result) {
    return RowFactory.create(
        startedAt,
        runId,
        RowType.CHECKPOINT.name(),
        null,
        null,
        null,
        result.getReferenceName(),
        result.getHashOnReference(),
        result.getLastLiveCommitHash(),
        null,
        null);
  }

  Map<String, String> collectLatestCommitCheckPoint() {
    // get the latest run id with marker row
    Dataset<Row> latestRunIdWithMarker =
        sql(
            "SELECT %s FROM %s WHERE %s = '%s' ORDER BY %s DESC LIMIT 1",
            COL_GC_RUN_ID,
            //
            catalogAndTableWithRefName,
            //
            COL_ROW_TYPE,
            RowType.CHECKPOINT_MARKER.name(),
            //
            COL_GC_RUN_START);

    if (latestRunIdWithMarker.isEmpty()) {
      return Collections.emptyMap();
    }

    String runId = latestRunIdWithMarker.collectAsList().get(0).getString(0);

    // collect all the checkpoint for the run id
    List<Row> rows =
        sql(
                "SELECT %s,%s,%s FROM %s WHERE %s = '%s' AND %s = '%s'",
                COL_REFERENCE_NAME,
                COL_HASH_ON_REFERENCE,
                COL_COMMIT_HASH,
                //
                catalogAndTableWithRefName,
                //
                COL_GC_RUN_ID,
                runId,
                //
                COL_ROW_TYPE,
                RowType.CHECKPOINT.name())
            .collectAsList();
    ImmutableMap.Builder<String, String> commitCheckPoints = ImmutableMap.builder();
    rows.forEach(row -> commitCheckPoints.put(row.getString(0), row.getString(2)));
    return commitCheckPoints.build();
  }

  static void createTableIfAbsent(
      SparkSession sparkSession,
      String catalogName,
      TableIdentifier tableIdentifier,
      String gcBranchName) {
    try {
      GCUtil.loadNessieCatalog(sparkSession, catalogName, gcBranchName)
          .createTable(tableIdentifier, IdentifiedResultsRepo.icebergSchema);
    } catch (AlreadyExistsException ex) {
      // Table can exist from previous GC run, no need to throw exception.
    }
  }

  private static String withRefName(String catalog, String identifier, String refName) {
    int tableNameIndex = identifier.lastIndexOf(".");
    String namespace = identifier.substring(0, tableNameIndex);
    String tableName = identifier.substring(tableNameIndex + 1);
    return catalog
        + "."
        + namespace
        + "."
        + ImmutableTableReference.builder().name(tableName).reference(refName).build();
  }

  private Timestamp getGcStartTime(String runId) {
    return sql(
            "SELECT %s FROM %s WHERE %s = '%s' AND %s = '%s' LIMIT 1",
            COL_GC_RUN_START,
            //
            catalogAndTableWithRefName,
            //
            COL_GC_RUN_ID,
            runId,
            //
            COL_ROW_TYPE,
            RowType.CONTENT_OUTPUT.name())
        .collectAsList()
        .get(0)
        .getTimestamp(0);
  }

  private Dataset<Row> getRowDataset(String runId, boolean isExpired) {
    return sql(
        "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' AND %s = %s",
        catalogAndTableWithRefName,
        //
        COL_GC_RUN_ID,
        runId,
        //
        COL_ROW_TYPE,
        RowType.CONTENT_OUTPUT.name(),
        //
        COL_IS_EXPIRED,
        isExpired);
  }

  @FormatMethod
  private Dataset<Row> sql(@FormatString String sqlStatement, Object... args) {
    String sql = String.format(sqlStatement, args);
    LOGGER.debug("Executing the sql -> {}", sql);
    return sparkSession.sql(sql);
  }
}
