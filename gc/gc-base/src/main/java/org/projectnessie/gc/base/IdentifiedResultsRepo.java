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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
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

  private static final StructType schema =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField(COL_GC_RUN_START, DataTypes.TimestampType, false),
            DataTypes.createStructField(COL_GC_RUN_ID, DataTypes.StringType, false),
            DataTypes.createStructField(COL_ROW_TYPE, DataTypes.StringType, false),
            DataTypes.createStructField(COL_CONTENT_ID, DataTypes.StringType, true),
            DataTypes.createStructField(COL_CONTENT_TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(COL_SNAPSHOT_ID, DataTypes.LongType, true),
            DataTypes.createStructField(COL_REFERENCE_NAME, DataTypes.StringType, true),
            DataTypes.createStructField(COL_HASH_ON_REFERENCE, DataTypes.StringType, true),
            DataTypes.createStructField(COL_COMMIT_HASH, DataTypes.StringType, true),
            DataTypes.createStructField(COL_METADATA_LOCATION, DataTypes.StringType, true),
            DataTypes.createStructField(COL_IS_EXPIRED, DataTypes.BooleanType, true)
          });

  private final SparkSession sparkSession;
  private final String catalogAndTableWithRefName;

  public IdentifiedResultsRepo(
      SparkSession sparkSession, String catalog, String gcBranchName, String gcTableIdentifier) {
    this.sparkSession = sparkSession;
    //TODO:
    this.catalogAndTableWithRefName = gcTableIdentifier;
    createTableIfAbsent(sparkSession, gcTableIdentifier);
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
    return getContentRowsForRunId(runId, true);
  }

  /**
   * Collect the live contents for the given run id as spark dataset.
   *
   * @param runId run id of completed identify task.
   * @return spark dataset of row where each row is having the live contents per content id per
   *     reference.
   */
  public Dataset<Row> collectLiveContentsAsDataSet(String runId) {
    return getContentRowsForRunId(runId, false);
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
    // unreferencedAssets
    //   .repartition(unreferencedAssets.col("tableName"))
    //   .sortWithinPartitions()
    //   .write()
    //   .format("iceberg")
    //   .mode("append")
    //   .save(table.toString());
    rowDataset.write().format("parquet").mode("append").save(catalogAndTableWithRefName);
    // try {
    //   // write content rows to the output table
    //   // rowDataset.writeTo(catalogAndTableWithRefName).append();
    // } catch (NoSuchTableException e) {
    //   throw new RuntimeException(
    //       "Problem while writing output rows to the table: " + catalogAndTableWithRefName, e);
    // }
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

  /**
   * Finds the latest completed checkpoint. Checkpoint is considered as completed when it has a
   * checkpoint marker row (written when the expiry operation is successful).
   *
   * @return a map of referenceName and checkpoint (its last traversed live commit hash from the
   *     head).
   */
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
                "SELECT %s,%s FROM %s WHERE %s = '%s' AND %s = '%s'",
                COL_REFERENCE_NAME,
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

    return rows.stream()
        .collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));
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

  private Dataset<Row> getContentRowsForRunId(String runId, boolean isExpired) {
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

  private void createTableIfAbsent(SparkSession session, String tableIdentifier) {
    int tableNameIndex = tableIdentifier.lastIndexOf(".");
    String namespace = tableIdentifier.substring(0, tableNameIndex);
    sql("CREATE DATABASE IF NOT EXISTS %s", namespace);

    CatalogPlugin catalog = session.sessionState().catalogManager().currentCatalog();

    String[] namespaces = {namespace};
    Identifier ident = Identifier.of(namespaces, tableIdentifier.substring(tableNameIndex + 1));

    try {
      Table table = ((TableCatalog) catalog).loadTable(ident);
      if (!table.schema().equals(schema)) {
        throw new RuntimeException(
          String.format("Cannot create table %s. Table with different schema already exists", ident));
      }
      // table already exists.
    } catch (NoSuchTableException e) {
      // table doesn't exist. So, create a table.
      try {
        ((TableCatalog) catalog).createTable(ident, schema, new Transform[0], ImmutableMap.of());
      } catch (TableAlreadyExistsException ex) {
        // can't happen as already verified before creating table.
      } catch (NoSuchNamespaceException ex) {
        // can't happen as manually creating the namespace before creating table.
      }
    }
  }
}
