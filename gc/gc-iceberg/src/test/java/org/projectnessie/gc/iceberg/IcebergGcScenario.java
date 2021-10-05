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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_CONTENT_ID;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_EXPIRED_DATA_FILES;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_EXPIRED_MANIFESTS;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_EXPIRED_MANIFEST_LISTS;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_REF_HASH;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_REF_NAME;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_SNAPSHOT_IDS;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_STATUS;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_SUCCESS;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_TABLE;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_TIMESTAMP;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.OUTPUT_VIA_HEAD;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.PROCEDURE_NAME;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.STATUS_NO_EXPIRE;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.STATUS_SNAPSHOTS_EXPIRED;
import static org.projectnessie.gc.iceberg.ExpireSnapshotsProcedure.toKey;
import static org.projectnessie.gc.iceberg.GcProcedures.NAMESPACE;
import static org.projectnessie.gc.iceberg.IcebergGcRepo.COL_GC_RUN_ID;
import static org.projectnessie.gc.iceberg.IcebergGcRepo.DEFAULT_GC_IDENTIFY_RUNS;
import static org.projectnessie.gc.iceberg.IcebergGcRepo.TYPE_GC_MARKER;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Nessie-Iceberg-GC test scenario builder and runner.
 *
 * <p>Nessie + Spark operations, change wall clock, change branches, etc and identify + expire
 * procedure calls are recorded. Recorded operations are executed via {@link
 * #runScenario(SparkSession, NessieApiV1)}.
 */
final class IcebergGcScenario {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergGcScenario.class);

  // TODO need to tweak `this.expectedCollects` (and probably the code of this class) to be able
  //  to have multiple `identifyProcedure` `expireProcedure` in a single scenario.

  private final String name;
  private final List<ExpectCollect> expectCollects = new ArrayList<>();
  private final List<Operation> operations = new ArrayList<>();

  IcebergGcScenario(String name) {
    this.name = name;
  }

  /** Run this scenario. */
  void runScenario(SparkSession sparkSession, NessieApiV1 api) {
    AbstractIcebergGc.TEST_CLOCK.setInstant(Instant.ofEpochSecond(0L));
    SparkSession.setActiveSession(sparkSession);

    for (Operation op : operations) {
      LOGGER.info(
          "References for '{}' before '{}':{}",
          op.desc,
          op.callSite.getStackTrace()[0],
          api.getAllReferences().get().stream()
              .map(r -> String.format("Reference at '%s' @ '%s'", r.getHash(), r.getName()))
              .collect(Collectors.joining("\n        ", "\n        ", "")));

      try {
        op.callee.accept(sparkSession, api);
      } catch (Error | RuntimeException e) {
        e.addSuppressed(op.callSite);
        throw e;
      } finally {
        LOGGER.info(
            "References for '{}' after '{}':{}",
            op.desc,
            op.callSite.getStackTrace()[0],
            api.getAllReferences().get().stream()
                .map(r -> String.format("Reference at '%s' @ '%s'", r.getHash(), r.getName()))
                .collect(Collectors.joining("\n        ", "\n        ", "")));
      }
    }
  }

  @Override
  public String toString() {
    return name;
  }

  private static final class Operation {
    final String desc;
    final Exception callSite;
    final BiConsumer<SparkSession, NessieApiV1> callee;

    Operation(String desc, Exception callSite, BiConsumer<SparkSession, NessieApiV1> callee) {
      this.desc = desc;
      this.callSite = callSite;
      this.callee = callee;
    }
  }

  private void addOp(
      BiConsumer<SparkSession, NessieApiV1> callee, String name, Object... formatParams) {
    String desc = String.format(name, formatParams);
    StackTraceElement[] st =
        Arrays.stream(new Exception().getStackTrace()).skip(2).toArray(StackTraceElement[]::new);
    Exception callSite = new Exception(desc);
    callSite.setStackTrace(st);
    operations.add(new Operation(desc, callSite, callee));
  }

  /**
   * Record the expected Nessie GC identify/expire procedure results at the current commits in the
   * mentioned branches.
   *
   * @param catalog catalog to inquire the content-id for the table
   * @param tableName table identifier
   * @param numLiveSnapshots the expected number of live snapshots
   * @param numCollectedSnapshots the expected number of collected snapshots
   */
  IcebergGcScenario expectCollect(
      String catalog,
      String tableName,
      int numLiveSnapshots,
      int numCollectedSnapshots,
      int numLiveMetadataPointers) {
    addOp(
        (spark, api) -> {
          String referenceName =
              spark.sparkContext().conf().get(String.format("spark.sql.catalog.%s.ref", catalog));

          IcebergTable icebergTable;
          try {
            ContentsKey key = toKey(tableName);
            Map<ContentsKey, Contents> contents =
                api.getContents().refName(referenceName).key(key).get();
            Optional<IcebergTable> tableOpt =
                Optional.ofNullable(contents.get(key)).flatMap(c -> c.unwrap(IcebergTable.class));
            assertThat(tableOpt)
                .describedAs("Table '%s' not found in %s", key, referenceName)
                .isPresent();
            icebergTable = tableOpt.get();
          } catch (NessieNotFoundException e) {
            throw new RuntimeException(e);
          }

          expectCollects.add(
              new ExpectCollect(
                  referenceName,
                  tableName,
                  icebergTable.getId(),
                  numLiveSnapshots,
                  numCollectedSnapshots,
                  numLiveMetadataPointers));
        },
        "expectCollect(%s, %d, %d, %d)",
        tableName,
        numLiveSnapshots,
        numCollectedSnapshots,
        numLiveMetadataPointers);
    return this;
  }

  /** Record the wall-clock change to the given time. */
  IcebergGcScenario setClock(Instant time) {
    addOp((spark, api) -> AbstractIcebergGc.TEST_CLOCK.setInstant(time), "setClock(%s)", time);
    return this;
  }

  /** Record a "create branch" operation. */
  IcebergGcScenario createBranch(String branch, String sourceRef) {
    addOp(
        (spark, api) -> {
          try {
            String hash = api.getReference().refName(sourceRef).get().getHash();
            api.createReference()
                .sourceRefName(sourceRef)
                .reference(Branch.of(branch, hash))
                .create();
          } catch (NessieNotFoundException | NessieConflictException e) {
            throw new RuntimeException(e);
          }
        },
        "createBranch(%s, %s)",
        branch,
        sourceRef);
    return this;
  }

  /** Call the given consumer at this point of the scenario. */
  IcebergGcScenario againstSpark(Consumer<SparkSession> sparkAction) {
    addOp((spark, api) -> sparkAction.accept(spark), "againstSpark");
    return this;
  }

  /** Change the current Nessie reference for the given catalog in the Spark CatalogPlugin. */
  IcebergGcScenario changeRef(String catalog, String branch) {
    addOp(
        (spark, api) -> initSparkCatalogPlugin(spark, catalog, branch),
        "changeRef(%s, %s)",
        catalog,
        branch);
    return this;
  }

  static void initSparkCatalogPlugin(SparkSession spark, String catalog, String branch) {
    CatalogPlugin catalogImpl = spark.sessionState().catalogManager().catalog(catalog);
    spark.sparkContext().conf().set(String.format("spark.sql.catalog.%s.ref", catalog), branch);
    Map<String, String> catalogConf =
        AbstractGcProcedure.catalogConfWithRef(spark, catalog, branch);
    catalogImpl.initialize(catalog, new CaseInsensitiveStringMap(catalogConf));
  }

  /**
   * Run the Nessie GC {@link IdentifyLiveSnapshotsProcedure} using the given catalog, default
   * cut-off timestamp and "identify branch".
   */
  IcebergGcScenario identifyProcedure(String catalog, Instant notLive) {
    addOp(
        (spark, api) -> callIdentifyProcedure(spark, api, catalog, notLive),
        "identifyProcedure(%s, %s)",
        catalog,
        notLive);
    return this;
  }

  /**
   * Run the Nessie GC {@link ExpireSnapshotsProcedure} using the given catalog, "identify branch"
   * and "expire branch".
   */
  IcebergGcScenario expireProcedure(String catalog, String expireBranch) {
    addOp(
        (spark, api) -> callExpireProcedure(spark, catalog, expireBranch),
        "expireProcedure(%s, %s)",
        catalog,
        expireBranch);
    return this;
  }

  private void callIdentifyProcedure(
      SparkSession sparkSession, NessieApiV1 api, String catalog, Instant notLive) {

    List<Row> runIdAndStarted =
        sparkSession
            .sql(
                String.format(
                    "CALL %s.%s.%s(cut_off_timestamp => %d)",
                    catalog,
                    NAMESPACE,
                    IdentifyLiveSnapshotsProcedure.PROCEDURE_NAME,
                    notLive.getEpochSecond()))
            .collectAsList();
    String runId = runIdAndStarted.get(0).getString(0);
    Timestamp started = runIdAndStarted.get(0).getTimestamp(1);

    TableIdentifier identifyTableWithCatalog =
        TableIdentifier.parse(String.format("%s.%s", catalog, DEFAULT_GC_IDENTIFY_RUNS));
    List<Row> gcResultRows =
        sparkSession
            .sql(
                String.format(
                    "SELECT * FROM %s WHERE %s = '%s'",
                    identifyTableWithCatalog, COL_GC_RUN_ID, runId))
            .collectAsList();

    LOGGER.info(
        "Result of expire-procedure:{}",
        gcResultRows.stream()
            .map(Row::toString)
            .collect(Collectors.joining("\n        ", "\n        ", "")));

    IcebergGcRecord markerRow = null;
    Map<String, List<IcebergGcRecord>> gcRowsByContentId = new HashMap<>();
    for (Row row : gcResultRows) {
      IcebergGcRecord record = IcebergGcRepo.gcRecordFromRow(row);
      if (record.getRowType().equals(TYPE_GC_MARKER)) {
        markerRow = record;
      } else {
        gcRowsByContentId
            .computeIfAbsent(record.getContentsId(), x -> new ArrayList<>())
            .add(record);
      }
    }

    assertThat(markerRow)
        .isEqualTo(
            ImmutableIcebergGcRecord.builder()
                .rowType(TYPE_GC_MARKER)
                .gcRunId(runId)
                .gcRunStart(started)
                .build());

    assertThat(expectCollects)
        .allSatisfy(
            ec -> {
              List<IcebergGcRecord> gcRecord = gcRowsByContentId.get(ec.contentId);
              assertThat(gcRecord)
                  .isNotNull()
                  .extracting(
                      gcr -> gcr.getLiveMetadataPointers().size(),
                      gcr ->
                          gcr.getReferencesWithHashToKeys().entrySet().stream()
                              .filter(
                                  e ->
                                      e.getKey().startsWith(String.format("%s#", ec.referenceName)))
                              .map(Entry::getValue)
                              .findFirst()
                              .get())
                  .containsExactly(tuple(ec.numLiveMetadataPointers, ec.tableName));
            });
  }

  private void callExpireProcedure(SparkSession sparkSession, String catalog, String expireBranch) {
    Seq<String> relevantOutputs =
        JavaConverters.collectionAsScalaIterable(
                Arrays.asList(
                    OUTPUT_CONTENT_ID,
                    OUTPUT_SUCCESS,
                    OUTPUT_SNAPSHOT_IDS,
                    OUTPUT_STATUS,
                    OUTPUT_TABLE,
                    OUTPUT_REF_NAME,
                    OUTPUT_REF_HASH,
                    OUTPUT_VIA_HEAD,
                    OUTPUT_TIMESTAMP,
                    OUTPUT_EXPIRED_DATA_FILES,
                    OUTPUT_EXPIRED_MANIFEST_LISTS,
                    OUTPUT_EXPIRED_MANIFESTS))
            .toSeq();
    List<Map<String, Object>> expireResult1 =
        sparkSession
            .sql(
                String.format(
                    "CALL %s.%s.%s(expire_branch => '%s', identify_runs => %d, do_expire => true, grace_time_millis => 0)",
                    catalog, NAMESPACE, PROCEDURE_NAME, expireBranch, 3))
            .collectAsList()
            .stream()
            .map(row -> JavaConverters.mapAsJavaMap(row.getValuesMap(relevantOutputs)))
            .collect(Collectors.toList());

    LOGGER.info(
        "Result of expire-procedure:{}",
        expireResult1.stream()
            .map(
                m ->
                    m.entrySet().stream()
                        .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(", ")))
            .collect(Collectors.joining("\n        ", "\n        ", "")));

    assertThat(expireResult1)
        .allSatisfy(
            exp -> {
              String contentId = (String) exp.get(OUTPUT_CONTENT_ID);
              String tableName = (String) exp.get(OUTPUT_TABLE);
              String refName = (String) exp.get(OUTPUT_REF_NAME);

              Optional<ExpectCollect> ecOpt =
                  expectCollects.stream()
                      .filter(
                          e ->
                              e.contentId.equals(contentId)
                                  && e.tableName.equals(tableName)
                                  && e.referenceName.equals(refName))
                      .findFirst();
              if (ecOpt.isPresent()) {
                ExpectCollect ec = ecOpt.get();
                assertThat(
                        Arrays.asList(
                            exp.get(OUTPUT_STATUS),
                            exp.get(OUTPUT_SUCCESS),
                            exp.get(OUTPUT_SNAPSHOT_IDS) != null
                                ? ((Seq<?>) exp.get(OUTPUT_SNAPSHOT_IDS)).length()
                                : null))
                    .containsExactly(
                        ec.numCollectedSnapshots > 0 ? STATUS_SNAPSHOTS_EXPIRED : STATUS_NO_EXPIRE,
                        ec.numCollectedSnapshots > 0,
                        ec.numCollectedSnapshots > 0 ? ec.numCollectedSnapshots : null);
              }
            });
    // Set the expected number of collected snapshots to 0, in case the expire-procedure is run
    // again, so that one does not wrongly assert on the (old) number of collected snapshots.
    expireResult1.forEach(
        exp -> {
          String contentId = (String) exp.get(OUTPUT_CONTENT_ID);
          String tableName = (String) exp.get(OUTPUT_TABLE);
          String refName = (String) exp.get(OUTPUT_REF_NAME);
          Boolean viaHead = (Boolean) exp.get(OUTPUT_VIA_HEAD);
          expectCollects.stream()
              .filter(
                  e ->
                      e.contentId.equals(contentId)
                          && e.tableName.equals(tableName)
                          && e.referenceName.equals(refName))
              .forEach(
                  ec -> {
                    if (viaHead != null && viaHead && ec.numCollectedSnapshots > 0) {
                      // Increment the expected number of live metadata-pointers, because the
                      // expire-action performs a commit against that table on the branch.
                      ec.numLiveMetadataPointers++;
                    }
                    // Expire-action collected the snapshots, further assertions shall not fail.
                    ec.numCollectedSnapshots = 0;
                  });
        });
  }
}
