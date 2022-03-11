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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;
import org.projectnessie.model.ImmutableTableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/** DDL + DML functionality for the "IdentifiedResult" table. */
public final class IdentifiedResultsRepo {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdentifiedResultsRepo.class);

  private static final String COL_TYPE = "type";
  private static final String COL_GC_RUN_START = "gcRunStart";
  private static final String COL_GC_RUN_ID = "gcRunId";
  private static final String COL_CONTENT_ID = "contentId";
  private static final String COL_CONTENT_TYPE = "contentType";
  private static final String COL_SNAPSHOT_ID = "snapshotId";
  private static final String COL_REFERENCE_NAME = "referenceName";
  private static final String COL_HASH_ON_REFERENCE = "hashOnReference";

  private static final String TYPE_CONTENT = "GC_CONTENT";
  private static final String TYPE_GC_MARKER = "GC_MARK";

  private final Schema icebergSchema =
      new Schema(
          Types.StructType.of(
                  // Type of information this row represents, marker row or content row.
                  // marker row indicates that identify task is completed for that run id
                  // and the results for that run id is consumable.
                  required(1, COL_TYPE, Types.StringType.get()),
                  // GC run start timestamp.
                  required(2, COL_GC_RUN_START, Types.TimestampType.withZone()),
                  // GC run-ID.
                  required(3, COL_GC_RUN_ID, Types.StringType.get()),
                  // Nessie Content.id
                  optional(4, COL_CONTENT_ID, Types.StringType.get()),
                  // Nessie Content.type
                  optional(5, COL_CONTENT_TYPE, Types.StringType.get()),
                  // Iceberg Table/View Content's snapshot/version id.
                  optional(6, COL_SNAPSHOT_ID, Types.LongType.get()),
                  // Name of the reference via which the contentID was collected
                  optional(7, COL_REFERENCE_NAME, Types.StringType.get()),
                  // Hash of the reference via which the contentID was collected
                  optional(8, COL_HASH_ON_REFERENCE, Types.StringType.get()))
              .fields());

  private final StructType schema = SparkSchemaUtil.convert(icebergSchema);

  private final SparkSession sparkSession;
  private final String catalogAndTableWithRefName;

  public IdentifiedResultsRepo(
      SparkSession sparkSession, String catalog, String gcRefName, String gcTable) {
    this.sparkSession = sparkSession;
    this.catalogAndTableWithRefName = withRefName(catalog, gcTable, gcRefName);
    createTableIfAbsent(sparkSession, catalog, TableIdentifier.parse(gcTable), gcRefName);
  }

  public StructType getSchema() {
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
    // collect marker row for the given run-id
    // Example Query:
    // SELECT gcRunId FROM nessie.db1.`identified_results@someGcRef`
    //    WHERE gcRunId = '1a088e65-a6ea-42e9-819d-025f4e22eaec' AND type = 'GC_MARK'
    Dataset<Row> markerRow =
        sql(
            "SELECT %s FROM %s WHERE %s = '%s' AND %s = '%s'",
            COL_GC_RUN_ID,
            //
            catalogAndTableWithRefName,
            //
            COL_GC_RUN_ID,
            runId,
            //
            COL_TYPE,
            TYPE_GC_MARKER);
    if (markerRow.isEmpty()) {
      return markerRow;
    }
    // Example Query:
    // SELECT * FROM nessie.db1.`identified_results@someGcRef`
    //    WHERE gcRunId = '9e809dc9-ac35-41c9-b1c0-5ebcf452ea2d' AND type = 'GC_CONTENT'
    return sql(
        "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'",
        catalogAndTableWithRefName,
        //
        COL_GC_RUN_ID,
        runId,
        //
        COL_TYPE,
        TYPE_CONTENT);
  }

  /**
   * Get the last written run id which is having a marker row.
   *
   * @return latest completed run id.
   */
  public String getLatestCompletedRunID() {
    // collect marker row for the last written run id
    // Example query:
    // SELECT gcRunId FROM nessie.db1.`identified_results@someGcRef
    //    WHERE type = 'GC_MARK' ORDER BY gcRunStart DESC LIMIT 1
    List<Row> markerRows =
        sql(
                "SELECT %s FROM %s WHERE %s = '%s' ORDER BY %s DESC LIMIT 1",
                COL_GC_RUN_ID,
                //
                catalogAndTableWithRefName,
                //
                COL_TYPE,
                TYPE_GC_MARKER,
                //
                COL_GC_RUN_START)
            .collectAsList();
    return markerRows.isEmpty() ? null : markerRows.get(0).getString(0);
  }

  /**
   * Convert each task result into a spark row and store it in the output table. Write a marker row
   * in the end to indicate that the task is completed.
   *
   * @param rowDataset content rows to be written.
   * @param runId run id of current identify task.
   * @param startedAt task start time.
   */
  void writeToOutputTable(Dataset<Row> rowDataset, String runId, Timestamp startedAt) {
    try {
      // write content rows
      rowDataset.writeTo(catalogAndTableWithRefName).append();
      // write marker row
      sparkSession
          .createDataFrame(Collections.singletonList(createMarkerRow(runId, startedAt)), schema)
          .writeTo(catalogAndTableWithRefName)
          .append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  static Row getContentRowVariablePart(
      String contentId, String contentType, long snapshotId, String refName, String hash) {
    return RowFactory.create(
        IdentifiedResultsRepo.TYPE_CONTENT,
        // the fixed value columns (runId and startTime)
        // will be added in the callers using dataframe.withColumn().
        // Hence they are filled as 'null' here.
        null,
        null,
        contentId,
        contentType,
        snapshotId,
        refName,
        hash);
  }

  private static Row createMarkerRow(String runID, Timestamp startedAt) {
    return RowFactory.create(
        IdentifiedResultsRepo.TYPE_GC_MARKER, startedAt, runID, null, null, null, null, null);
  }

  private void createTableIfAbsent(
      SparkSession sparkSession,
      String catalogName,
      TableIdentifier tableIdentifier,
      String gcRefName) {
    // get Nessie catalog
    Catalog nessieCatalog =
        CatalogUtil.loadCatalog(
            NessieCatalog.class.getName(),
            catalogName,
            catalogConfWithRef(sparkSession, catalogName, gcRefName),
            sparkSession.sparkContext().hadoopConfiguration());
    try {
      nessieCatalog.createTable(tableIdentifier, icebergSchema);
    } catch (AlreadyExistsException ex) {
      // Table can exist from previous GC run, no need to  throw exception.
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

  private static Map<String, String> catalogConfWithRef(
      SparkSession spark, String catalog, String branch) {
    Stream<Tuple2<String, String>> conf =
        Arrays.stream(
            spark
                .sparkContext()
                .conf()
                .getAllWithPrefix(String.format("spark.sql.catalog.%s.", catalog)));
    if (branch != null) {
      conf = conf.map(t -> t._1.equals("ref") ? Tuple2.apply(t._1, branch) : t);
    }
    return conf.collect(Collectors.toMap(t -> t._1, t -> t._2));
  }

  private Dataset<Row> sql(String sqlStatement, Object... args) {
    String sql = String.format(sqlStatement, args);
    LOGGER.debug("Executing the sql -> {}", sql);
    return sparkSession.sql(sql);
  }
}
