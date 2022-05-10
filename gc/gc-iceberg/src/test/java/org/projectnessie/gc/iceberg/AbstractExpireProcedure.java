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
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.gc.iceberg.GcProcedureUtil.NAMESPACE;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.commit;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.createTable;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.dropTable;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.useReference;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.gc.base.AbstractRestGC;
import org.projectnessie.gc.base.IdentifiedResultsRepo;
import org.projectnessie.model.Branch;

public abstract class AbstractExpireProcedure extends AbstractRestGC {

  @TempDir static File LOCAL_DIR;

  static final String CATALOG_NAME = "nessie";
  static final String GC_BRANCH_NAME = "gcRef";
  static final String GC_EXPIRE_BRANCH_NAME = "someExpireRef";
  static final String GC_OUTPUT_TABLE_NAME = "gc_results";
  static final String GC_SPARK_CATALOG = "org.projectnessie.gc.iceberg.NessieIcebergGcSparkCatalog";

  static final String TABLE_ONE = "table_1";
  static final String TABLE_TWO = "table_2";
  static final String TABLE_THREE = "table_3";

  @Override
  protected SparkSession getSparkSession() {
    return ProcedureTestUtil.getSessionWithGcCatalog(
        getUri().toString(), LOCAL_DIR.toURI().toString(), GC_SPARK_CATALOG);
  }

  @Test
  public void testDryRun() {
    // ------  Time ---- | --- branch1 --------------|
    //         t0        | Create branch             |
    //         t1        | TABLE_ONE : ID_1 (expired)|
    //         t2        | TABLE_ONE : ID_2 (expired)|
    //         t3        | DROP branch               |
    //         t4        |-- cut off time -----------|
    String prefix = "dry_run";
    String branch1 = prefix + "_1";
    try (SparkSession sparkSession = getSparkSession()) {
      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch1, "main");
      useReference(sparkSession, CATALOG_NAME, branch1);
      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      List<Row> expectedExpired = new ArrayList<>();
      fillExpectedContents(Branch.of(branch1, null), 2, expectedExpired);

      ProcedureTestUtil.dropBranch(sparkSession, CATALOG_NAME, branch1);

      Instant cutoffTime = Instant.now();

      Dataset<Row> dataset =
          performGcAndVerify(
              sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      performExpiry(prefix, sparkSession, perContentId(dataset), true);
    }
  }

  @Test
  public void testMultiRefMultipleSharedTables() {
    // ------  Time ---- | --- branch1 -----| ---- branch2 -----| --- branch3 ------------- |
    //         t0        | create branch    |                   |                           |
    //         t1        | TABLE_ONE : ID_1 | {TABLE_ONE : ID_1}| {TABLE_ONE : ID_1}        |
    //         t2        |                  |  create branch    |                           |
    //         t3        | TABLE_TWO : ID_1 |                   | {TABLE_TWO : ID_1}        |
    //         t4        |                  |                   | create branch             |
    //         t5        |                  |                   | TABLE_THREE : ID_1 (expired)|
    //         t6        |                  |  TABLE_ONE : ID_2 |                           |
    //         t7        | DROP TABLE_ONE   |                   |                           |
    //         t8        |                  |                   | DROP TABLE_TWO            |
    //         t9        |                  |                   | DROP TABLE_THREE          |
    //         t10       |-- cut off time --|-- cut off time -- |-- cut off time -- --------|
    //         t11       | TABLE_TWO : ID_3 |                   |                           |
    //         t12       |                  |                   | TABLE_ONE : ID_3          |
    String prefix = "ExpireMultiRefMultipleSharedTables";
    String branch1 = prefix + "_1";
    String branch2 = prefix + "_2";
    String branch3 = prefix + "_3";
    try (SparkSession sparkSession = getSparkSession()) {
      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch1, "main");
      useReference(sparkSession, CATALOG_NAME, branch1);
      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch2, branch1);
      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch3, branch1);

      useReference(sparkSession, CATALOG_NAME, branch3);
      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_THREE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_THREE);

      List<Row> expectedExpired = new ArrayList<>();
      fillExpectedContents(Branch.of(branch3, null), 1, expectedExpired);

      useReference(sparkSession, CATALOG_NAME, branch2);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      useReference(sparkSession, CATALOG_NAME, branch1);
      dropTable(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      useReference(sparkSession, CATALOG_NAME, branch3);
      dropTable(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      dropTable(sparkSession, CATALOG_NAME, prefix, TABLE_THREE);

      Instant cutoffTime = Instant.now();

      useReference(sparkSession, CATALOG_NAME, branch1);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);

      useReference(sparkSession, CATALOG_NAME, branch3);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      Dataset<Row> dataset =
          performGcAndVerify(
              sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      performExpiry(prefix, sparkSession, perContentId(dataset), false);
    }
  }

  @Test
  public void testSharedTablesWithTag() {
    // -- Time --| --- branch1 ------------   | ---- tag1    ----- | ------ tag2    ------------  |
    //   t0     | create branch              |                    |                              |
    //   t1     | TABLE_ONE : ID_1           | {TABLE_ONE : ID_1} | {TABLE_ONE : ID_1}           |
    //   t2     |                            |  create tag        |                              |
    //   t3     | TABLE_TWO : ID_1 (expired) |                    | {TABLE_TWO : ID_1} (expired) |
    //   t4     |                            |                    | create tag                   |
    //   t5     | DROP TABLE_ONE             |                    |                              |
    //   t6     |                            |                    | drop tag                     |
    //   t7     | TABLE_TWO : ID_2 (expired) |                    |                              |
    //   t8     | TABLE_TWO : ID_3           |                    |                              |
    //   t9     |-- cut off time ------------|-- cut off time --  |-- cut off time -- --   -- -- |
    String prefix = "ExpireSharedTablesWithTag";
    String branch1 = prefix + "_1";
    String tag1 = prefix + "_2";
    String tag2 = prefix + "_3";
    try (SparkSession sparkSession = getSparkSession()) {
      List<Row> expectedExpired = new ArrayList<>();
      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch1, "main");

      useReference(sparkSession, CATALOG_NAME, branch1);
      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      ProcedureTestUtil.createTag(sparkSession, CATALOG_NAME, tag1, branch1);

      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      fillExpectedContents(Branch.of(branch1, null), 1, expectedExpired);

      ProcedureTestUtil.createTag(sparkSession, CATALOG_NAME, tag2, branch1);
      fillExpectedContents(Branch.of(tag2, null), 1, expectedExpired);

      dropTable(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      ProcedureTestUtil.dropTag(sparkSession, CATALOG_NAME, tag2);

      commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      fillExpectedContents(Branch.of(branch1, null), 1, expectedExpired);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);

      Instant cutoffTime = Instant.now();

      Dataset<Row> dataset =
          performGcAndVerify(
              sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      performExpiry(prefix, sparkSession, perContentId(dataset), false);
    }
  }

  @Test
  public void testCheckPoint() {
    // ------  Time ---- | --- branch1 --------------|
    //         t0        | Create branch             |
    //         t1        | TABLE_ONE : ID_1 (expired)|
    //         t2        | TABLE_ONE : ID_2 (expired)|
    //         t3        | TABLE_ONE : ID_3          |
    //         t4        |-- cut off time -----------|
    String prefix = "checkpoint";
    String branch1 = prefix + "_1";
    try (SparkSession sparkSession = getSparkSession()) {
      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch1, "main");
      useReference(sparkSession, CATALOG_NAME, branch1);
      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      List<Row> expectedExpired = new ArrayList<>();
      fillExpectedContents(Branch.of(branch1, null), 2, expectedExpired);

      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      Instant cutoffTime = Instant.now();

      Dataset<Row> dataset =
          performGcAndVerify(
              sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      performExpiry(prefix, sparkSession, perContentId(dataset), false);
      Dataset<Row> dataset1 =
          performGcAndVerify(
              sparkSession,
              prefix,
              cutoffTime,
              Collections.emptyMap(),
              Collections.emptyList(),
              null);

      // clear the checkpoint point
      useReference(sparkSession, CATALOG_NAME, GC_BRANCH_NAME);
      ProcedureTestUtil.sql(
          sparkSession,
          "DELETE FROM %s WHERE %s = '%s' OR %s = '%s'",
          CATALOG_NAME + "." + prefix + "." + GC_OUTPUT_TABLE_NAME,
          "rowType",
          "checkpoint",
          "rowType",
          "checkpoint-marker");

      // should collect the expired contents from the beginning of time.
      dataset = performGc(sparkSession, prefix, Instant.now(), Collections.emptyMap(), null);
      // identify branch and expire branch will have some expired contents.
      // Expire branch's commit hash is not visible outside as the reference is dropped after
      // expiry.
      // Hence, only validate original references.
      dataset = dataset.filter(dataset.col("referenceName").equalTo("checkpoint_1"));
      verify(dataset, expectedExpired, sparkSession, IdentifiedResultsRepo.getSchema());
      // TODO: try to run the expiry, output should be empty.
      // performExpiry(prefix, sparkSession, sparkSession.emptyDataFrame(), false);
    }
  }

  @Test
  public void testSharedTableMultipleRef() {
    // -- Time --| --- branch1 ------------   | ---- branch2 --------------  |
    //   t0     | create branch              |                              |
    //   t1     | TABLE_ONE : ID_1 (expired) | {TABLE_ONE : ID_1} (expired) |
    //   t2     |                            |  create branch               |
    //   t3     | TABLE_ONE : ID_2 (expired) |                              |
    //   t4     | TABLE_ONE : ID_3 (expired) |                              |
    //   t5     | TABLE_ONE : ID_4           |                              |
    //   t6     |                            | TABLE_ONE : ID_5 (expired)   |
    //   t7     |                            | TABLE_ONE : ID_6 (expired)   |
    //   t8     |                            | TABLE_ONE : ID_7             |
    //   t9     |-- cut off time ------------|-- cut off time ------------- |
    String prefix = "ExpireSharedTableMultipleRef";
    String branch1 = prefix + "_1";
    String branch2 = prefix + "_2";
    try (SparkSession sparkSession = getSparkSession()) {
      List<Row> expectedExpired = new ArrayList<>();
      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch1, "main");

      useReference(sparkSession, CATALOG_NAME, branch1);
      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch2, branch1);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      fillExpectedContents(Branch.of(branch1, null), 3, expectedExpired);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      useReference(sparkSession, CATALOG_NAME, branch2);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      fillExpectedContents(Branch.of(branch2, null), 3, expectedExpired);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      Instant cutoffTime = Instant.now();

      Dataset<Row> dataset =
          performGcAndVerify(
              sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);
      dataset.show(false);
      // TODO: need to fix the expiry algorithm for without global state.
      performExpiry(prefix, sparkSession, perContentId(dataset), false);
    }
  }

  private void performExpiry(
      String prefix, SparkSession sparkSession, Dataset<Row> rows, Boolean dryRun) {
    Dataset<Row> output =
        sparkSession.sql(
            String.format(
                "CALL %s.%s.%s("
                    + "expire_procedure_branch_name => '%s', "
                    + "nessie_catalog_name => '%s', "
                    + "output_branch_name => '%s', "
                    + "output_table_identifier => '%s', "
                    + "nessie_client_configurations => map('%s','%s'), "
                    + "dry_run => %s)",
                CATALOG_NAME,
                NAMESPACE,
                ExpireSnapshotsProcedure.PROCEDURE_NAME,
                //
                GC_EXPIRE_BRANCH_NAME,
                CATALOG_NAME,
                GC_BRANCH_NAME,
                prefix + "." + GC_OUTPUT_TABLE_NAME,
                CONF_NESSIE_URI,
                getUri().toString(),
                dryRun));
    output.show(200, false);
    verifyExpiry(output, rows, dryRun);
  }

  private Dataset<Row> performGcAndVerify(
      SparkSession session,
      String prefix,
      Instant cutoffTimeStamp,
      Map<String, Instant> cutOffTimeStampPerRef,
      List<Row> expectedDataSet,
      Instant deadReferenceCutoffTime) {
    String runId =
        ProcedureTestUtil.performGcWithProcedure(
            session,
            CATALOG_NAME,
            GC_BRANCH_NAME,
            prefix + "." + GC_OUTPUT_TABLE_NAME,
            getUri().toString(),
            cutoffTimeStamp,
            deadReferenceCutoffTime,
            cutOffTimeStampPerRef);
    IdentifiedResultsRepo actualIdentifiedResultsRepo =
        new IdentifiedResultsRepo(
            session, CATALOG_NAME, GC_BRANCH_NAME, prefix + "." + GC_OUTPUT_TABLE_NAME);
    Dataset<Row> actualRowDataset =
        actualIdentifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
    verify(actualRowDataset, expectedDataSet, session, IdentifiedResultsRepo.getSchema());
    return actualRowDataset;
  }

  private Dataset<Row> performGc(
      SparkSession session,
      String prefix,
      Instant cutoffTimeStamp,
      Map<String, Instant> cutOffTimeStampPerRef,
      Instant deadReferenceCutoffTime) {
    String runId =
        ProcedureTestUtil.performGcWithProcedure(
            session,
            CATALOG_NAME,
            GC_BRANCH_NAME,
            prefix + "." + GC_OUTPUT_TABLE_NAME,
            getUri().toString(),
            cutoffTimeStamp,
            deadReferenceCutoffTime,
            cutOffTimeStampPerRef);
    IdentifiedResultsRepo actualIdentifiedResultsRepo =
        new IdentifiedResultsRepo(
            session, CATALOG_NAME, GC_BRANCH_NAME, prefix + "." + GC_OUTPUT_TABLE_NAME);
    return actualIdentifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
  }

  private Dataset<Row> perContentId(Dataset<Row> rows) {
    return rows.groupBy("contentId")
        .agg(functions.collect_set("snapshotId").as("snapshotIds"))
        .select("contentId", "snapshotIds")
        .withColumn("snapshotsCount", functions.size(functions.col("snapshotIds")));
  }

  private void verifyExpiry(Dataset<Row> actual, Dataset<Row> dfExpected, boolean dryRun) {
    Dataset<Row> dfActual =
        actual.select("content_id", "snapshot_ids", "deleted_manifest_lists_count");
    // when both the dataframe is same, df.except() should return empty.
    assertThat(dfActual.count()).isEqualTo(dfExpected.count());
    // TODO: remove later
    dfActual.show(200, false);
    dfExpected.show(200, false);
    assertThat(dfExpected.except(dfActual).collectAsList()).isEmpty();

    try {
      FileSystem localFs = FileSystem.getLocal(new Configuration());
      List<Row> deletedFilesList = actual.select("deleted_files_list").collectAsList();
      deletedFilesList.stream()
          .map(row -> row.getList(0))
          .forEach(
              files -> {
                files.forEach(
                    file -> {
                      try {
                        assertThat(localFs.exists(new Path((String) file))).isEqualTo(dryRun);
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    });
              });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
