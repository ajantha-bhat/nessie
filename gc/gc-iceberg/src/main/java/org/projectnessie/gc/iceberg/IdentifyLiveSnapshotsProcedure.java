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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.versioned.persist.gc.ContentsValuesCollector;
import org.projectnessie.versioned.persist.gc.GC;
import org.projectnessie.versioned.persist.gc.GCResult;
import org.projectnessie.versioned.persist.gc.IcebergContentsValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nessie GC procedure to identify the live snapshots and contents via the {@link GC Base-GC
 * functionality}, writes via {@link IcebergGcRepo}.
 */
public class IdentifyLiveSnapshotsProcedure extends AbstractGcProcedure {

  public static final String PROCEDURE_NAME = "identify";

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IdentifyLiveSnapshotsProcedure.class);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        // Hint: this is in microseconds since epoch, as usual in Spark
        ProcedureParameter.required("cut_off_timestamp", DataTypes.LongType),
        ProcedureParameter.optional("read_until", DataTypes.LongType),
        ProcedureParameter.optional("gc_table", DataTypes.StringType),
        ProcedureParameter.optional("writer_batch_size", DataTypes.IntegerType),
        ProcedureParameter.optional(
            "reference_cut_off_timestamps",
            // Hint: this is in seconds since epoch
            DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType)),
        ProcedureParameter.optional("catalog", DataTypes.StringType),
        ProcedureParameter.optional("gc_branch", DataTypes.StringType)
      };

  public static final String OUTPUT_RUN_ID = "run_id";
  public static final String OUTPUT_STARTED = "started";

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(OUTPUT_RUN_ID, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OUTPUT_STARTED, DataTypes.TimestampType, true, Metadata.empty())
          });

  private InternalRow resultRow(String runId, Timestamp started) {
    return internalRow(runId, DateTimeUtils.fromJavaTimestamp(started));
  }

  public IdentifyLiveSnapshotsProcedure(TableCatalog currentCatalog) {
    super(currentCatalog);
  }

  @Override
  protected String procedureName() {
    return PROCEDURE_NAME;
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public String description() {
    return "Identifies all live snapshots, but does not expire Iceberg snapshots.";
  }

  @Override
  public InternalRow[] call(InternalRow internalRow) {
    Instant cutOffTimestampSecondsSinceEpoch = Instant.ofEpochSecond(internalRow.getLong(0));
    long readCommitsUntil = internalRow.isNullAt(3) ? 0L : internalRow.getLong(1);
    if (!internalRow.isNullAt(2)) {
      setGcTable(internalRow.getString(2));
    }
    if (!internalRow.isNullAt(3)) {
      setWriterBatchSize(internalRow.getInt(3));
    }
    Map<String, Instant> perReferenceCutoffs = new HashMap<>();
    if (!internalRow.isNullAt(4)) {
      MapData map = internalRow.getMap(4);
      for (int i = 0; i < map.numElements(); i++) {
        String refName = map.keyArray().getUTF8String(i).toString();
        Instant cutOffTimestamp = Instant.ofEpochSecond(map.valueArray().getLong(i));
        perReferenceCutoffs.put(refName, cutOffTimestamp);
      }
    }
    if (!internalRow.isNullAt(5)) {
      setCatalog(internalRow.getString(5));
    }
    if (!internalRow.isNullAt(6)) {
      setBranch(internalRow.getString(6));
    }

    try (NessieCatalog nessieCatalog = createNessieGcCatalog()) {
      GC gc =
          GC.builder()
              .withApi(nessieCatalog.getApi())
              .withContentTypeInclusionPredicate(c -> c == Contents.Type.ICEBERG_TABLE)
              .withDefaultLiveAfterValue(cutOffTimestampSecondsSinceEpoch)
              .withReadUntilCommitTimestamp(
                  Instant.ofEpochSecond(
                      TimeUnit.MICROSECONDS.toSeconds(readCommitsUntil),
                      TimeUnit.MICROSECONDS.toNanos(
                          readCommitsUntil % TimeUnit.SECONDS.toMicros(1))))
              .addLiveAfterComputation(r -> perReferenceCutoffs.get(r.getName()))
              .build();

      String runId = UUID.randomUUID().toString();
      Timestamp started = Timestamp.from(Instant.now());

      return new InternalRow[] {performGarbageIdentification(runId, started, gc)};
    }
  }

  private InternalRow performGarbageIdentification(String runId, Timestamp started, GC gc) {
    LOGGER.info("GC run {}: Performing garbage identification...", runId);

    GCResult<IcebergContentsValues> gcResult =
        gc.performGC(new ContentsValuesCollector<>(IcebergContentsValues::new));

    LOGGER.info(
        "GC run {}: Garbage identification finished for {} tables, persisting information ...",
        runId,
        gcResult.getContentsValues().size());

    try (IcebergGcRepo repo = openRepo()) {
      for (Entry<String, IcebergContentsValues> cidAndValues :
          gcResult.getContentsValues().entrySet()) {
        String cid = cidAndValues.getKey();
        IcebergContentsValues collectables = cidAndValues.getValue();

        IcebergGcRecord gcRecord =
            ImmutableIcebergGcRecord.builder()
                .rowType(IcebergGcRepo.TYPE_CONTENT)
                .contentsId(cid)
                .liveMetadataPointers(collectables.getLiveMetadataPointers())
                .referencesWithHashToKeys(
                    collectables.getReferencesToKeyAndHash().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                e -> String.format("%s#%s", e.getKey(), e.getValue().getHash()),
                                e -> toIdentifier(e.getValue().getKey()).toString())))
                .gcRunStart(started)
                .gcRunId(runId)
                .build();
        repo.addRecord(gcRecord);
      }

      IcebergGcRecord runRecord =
          ImmutableIcebergGcRecord.builder()
              .rowType(IcebergGcRepo.TYPE_GC_MARKER)
              .gcRunStart(started)
              .gcRunId(runId)
              .build();
      repo.addRecord(runRecord);

      LOGGER.info(
          "GC run {}: Wrote information about {} tables",
          runId,
          gcResult.getContentsValues().size());

      return resultRow(runId, started);
    }
  }

  static TableIdentifier toIdentifier(ContentsKey key) {
    List<String> elements = key.getElements();
    return TableIdentifier.of(elements.toArray(new String[0]));
  }
}
