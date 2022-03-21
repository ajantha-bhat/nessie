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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;

/** DDL + DML functionality for the "GCCheckPoint" table. */
public final class GCCheckPointRepo extends BaseRepo {

  private static final String COL_GC_RUN_START = "gcRunStart";
  private static final String COL_GC_RUN_ID = "gcRunId";
  private static final String COL_REFERENCE_NAME = "referenceName";
  private static final String COL_HASH_ON_REFERENCE = "hashOnReference";
  private static final String COL_COMMIT_HASH = "commitHash";
  private static final String COL_ROW_TYPE = "rowType";

  private static final String TYPE_MARKER = "MARKER";
  private static final String TYPE_DATA = "DATA";

  private final Schema icebergSchema =
      new Schema(
          Types.StructType.of(
                  // GC run start timestamp.
                  required(1, COL_GC_RUN_START, Types.TimestampType.withZone()),
                  // GC run-ID.
                  required(2, COL_GC_RUN_ID, Types.StringType.get()),
                  // To indicate whether the row is of type data or marker.
                  required(3, COL_ROW_TYPE, Types.StringType.get()),
                  // Name of the reference for which this row info belongs to.
                  optional(4, COL_REFERENCE_NAME, Types.StringType.get()),
                  // Hash of the reference head for which this row info belongs to.
                  optional(5, COL_HASH_ON_REFERENCE, Types.StringType.get()),
                  // Last visited commit hash for live contents traversal.
                  optional(6, COL_COMMIT_HASH, Types.StringType.get()))
              .fields());

  private final StructType schema = SparkSchemaUtil.convert(icebergSchema);

  public GCCheckPointRepo(
      SparkSession sparkSession,
      String catalog,
      String gcRefName,
      String gcCheckPointTableIdentifier) {
    super(sparkSession, catalog, gcRefName, gcCheckPointTableIdentifier);
    createTableIfAbsent(
        sparkSession,
        catalog,
        TableIdentifier.parse(gcCheckPointTableIdentifier),
        gcRefName,
        icebergSchema);
  }

  public StructType getSchema() {
    return schema;
  }

  static Row createCheckPointRow(String runId, Timestamp startedAt, LiveContentsResult result) {
    return RowFactory.create(
        startedAt,
        runId,
        TYPE_DATA,
        result.getReferenceName(),
        result.getHashOnReference(),
        result.getLastLiveCommitHash());
  }

  public Row createMarkerRow(String runId) {
    return RowFactory.create(getGcStartTime(runId), runId, TYPE_MARKER, null, null, null);
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
            TYPE_MARKER,
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
                TYPE_DATA)
            .collectAsList();
    Map<String, String> commitCheckPoints = new HashMap<>();
    rows.forEach(row -> commitCheckPoints.put(row.getString(0), row.getString(2)));
    return commitCheckPoints;
  }

  public void writeToOutputTable(List<Row> rows) {
    try {
      Dataset<Row> rowDataset = sparkSession.createDataFrame(rows, schema);
      rowDataset.writeTo(catalogAndTableWithRefName).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          "Problem while writing gc history rows to the table: " + catalogAndTableWithRefName, e);
    }
  }

  private Timestamp getGcStartTime(String runId) {
    return sql(
            "SELECT %s FROM %s WHERE %s = '%s' LIMIT 1",
            COL_GC_RUN_START,
            //
            catalogAndTableWithRefName,
            //
            COL_GC_RUN_ID,
            runId)
        .collectAsList()
        .get(0)
        .getTimestamp(0);
  }
}
