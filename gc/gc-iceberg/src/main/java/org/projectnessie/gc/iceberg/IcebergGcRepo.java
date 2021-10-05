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
package org.projectnessie.gc.iceberg;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

/** DDL + DML functionality for the "identified live snapshots" table. */
final class IcebergGcRepo implements AutoCloseable {

  static final String COL_TYPE = "type";
  static final String COL_GC_RUN_ID = "gcRunId";
  static final String COL_GC_RUN_START = "gcRunStart";
  static final String COL_CONTENTS_ID = "contentsId";
  static final String COL_LIVE_METADATA_POINTERS = "liveMetadataPointers";
  static final String COL_REFERENCES_TO_KEYS = "referencesToKeys";

  static final String TYPE_GC_MARKER = "GC_MARK";
  static final String TYPE_CONTENT = "CONTENT";

  static final String SCHEMA_DDL =
      "CREATE TABLE %s (\n"
          + "  "
          + COL_TYPE
          + " string COMMENT 'Type of information this row represents',\n"
          + "  "
          + COL_GC_RUN_START
          + " timestamp COMMENT 'GC run start timestamp',\n"
          + "  "
          + COL_GC_RUN_ID
          + " string COMMENT 'GC run-ID',\n"
          + "  "
          + COL_CONTENTS_ID
          + " string COMMENT 'Nessie Contents.id',\n"
          + "  "
          + COL_LIVE_METADATA_POINTERS
          + " array<string> COMMENT 'Iceberg live metadata pointers, can be empty',\n"
          + "  "
          + COL_REFERENCES_TO_KEYS
          + " map<string, string> COMMENT 'Map of reference-names to content-keys'\n"
          + ")\n"
          + "USING iceberg\n"
          + "PARTITIONED BY ("
          + COL_GC_RUN_START
          + ")";

  static final int DEFAULT_BATCH_SIZE = 250;

  static final String DEFAULT_GC_IDENTIFY_RUNS = "nessie_gc.identify_garbage";

  private final SparkSession sparkSession;
  private final String catalogAndTable;
  private final int batchSize;
  private final List<Row> rows;
  private final StructType schema;

  IcebergGcRepo(SparkSession sparkSession, int batchSize, String catalog, TableIdentifier gcTable) {
    this.sparkSession = sparkSession;
    this.catalogAndTable = String.format("%s.%s", catalog, gcTable);
    this.batchSize = batchSize;
    this.rows = new ArrayList<>(batchSize);

    this.schema = checkTable(sparkSession, catalog, gcTable, catalogAndTable);
    // TODO need a better schema/partitioning to allow two things:
    //  - "point-ish" lookup of a specific identify-run via COL_GC_RUN_ID
    //  - retrieval of N most recent runs
    //  --> some composite of COL_GC_RUN_START + COL_GC_RUN_ID
  }

  void addRecord(IcebergGcRecord record) {
    rows.add(toRow(record));
    if (rows.size() == batchSize) {
      flush();
    }
  }

  void flush() {
    if (rows.isEmpty()) {
      return;
    }
    ArrayList<Row> batch = new ArrayList<>(rows);
    rows.clear();
    try {
      sparkSession.createDataFrame(batch, schema).writeTo(catalogAndTable).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    flush();
  }

  static Row toRow(IcebergGcRecord record) {
    switch (record.getRowType()) {
      case TYPE_GC_MARKER:
        return RowFactory.create(
            record.getRowType(), record.getGcRunStart(), record.getGcRunId(), null, null, null);
      case TYPE_CONTENT:
        return RowFactory.create(
            record.getRowType(),
            record.getGcRunStart(),
            record.getGcRunId(),
            record.getContentsId(),
            record.getLiveMetadataPointers(),
            record.getReferencesWithHashToKeys());
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row-type '%s'", record.getRowType()));
    }
  }

  static IcebergGcRecord gcRecordFromRow(Row row) {
    String rowType = row.getString(0);

    ImmutableIcebergGcRecord.Builder b =
        ImmutableIcebergGcRecord.builder()
            .rowType(rowType)
            .gcRunStart(row.getTimestamp(1))
            .gcRunId(row.getString(2));

    switch (rowType) {
      case TYPE_GC_MARKER:
        break;
      case TYPE_CONTENT:
        b.contentsId(row.getString(3))
            .liveMetadataPointers(row.getList(4))
            .referencesWithHashToKeys(row.getJavaMap(5));
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown row-type '%s'", rowType));
    }

    return b.build();
  }

  Dataset<Row> collectExpireableSnapshots(int identifyRuns) {
    // Subquery to collect the last 'identifyRuns' last GC-run-IDs
    String mostRecentRunIds =
        String.format(
            "SELECT %s FROM %s WHERE %s = '%s' ORDER BY %s DESC LIMIT %d",
            COL_GC_RUN_ID,
            catalogAndTable,
            COL_TYPE,
            TYPE_GC_MARKER,
            COL_GC_RUN_START,
            identifyRuns);

    // Subquery to collect all content-id + snapshot-id tuples that represent a live snapshot
    // seen in at least one of the 'identifyRuns' last GC runs.
    String liveSnapshotIdsQuery =
        String.format(
            "SELECT DISTINCT %s FROM %s WHERE %s IN (%s) AND %s = '%s'",
            COL_CONTENTS_ID,
            catalogAndTable,
            COL_GC_RUN_ID,
            mostRecentRunIds,
            COL_TYPE,
            TYPE_CONTENT);

    String liveSnapshotDetails =
        String.format(
            "SELECT details.%s, details.%s, details.%s \n"
                + "FROM %s details, (%s) snapshots \n"
                + "WHERE details.%s = snapshots.%s \n"
                + "ORDER BY details.%s",
            COL_CONTENTS_ID,
            COL_LIVE_METADATA_POINTERS,
            COL_REFERENCES_TO_KEYS,
            //
            catalogAndTable,
            liveSnapshotIdsQuery,
            //
            COL_CONTENTS_ID,
            COL_CONTENTS_ID,
            //
            COL_CONTENTS_ID);

    return sparkSession.sql(liveSnapshotDetails);
  }

  private static StructType checkTable(
      SparkSession sparkSession,
      String catalogName,
      TableIdentifier tableIdentifier,
      String catalogAndTable) {
    CatalogPlugin catalog = sparkSession.sessionState().catalogManager().catalog(catalogName);
    Identifier ident = Identifier.of(tableIdentifier.namespace().levels(), tableIdentifier.name());
    try {
      return ((TableCatalog) catalog).loadTable(ident).schema();
    } catch (NoSuchTableException ex) {
      sparkSession.sql(String.format(SCHEMA_DDL, catalogAndTable));
      try {
        return ((TableCatalog) catalog).loadTable(ident).schema();
      } catch (NoSuchTableException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
