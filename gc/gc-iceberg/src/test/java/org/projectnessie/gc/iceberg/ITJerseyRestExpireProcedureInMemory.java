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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.tuple;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.gc.iceberg.GCProcedureUtil.NAMESPACE;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.commit;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.createTable;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.dropTable;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.useReference;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.gc.base.AbstractRestGC;
import org.projectnessie.gc.base.IdentifiedResultsRepo;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.jaxrs.ext.NessieUri;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
@ExtendWith(DatabaseAdapterExtension.class)
public class ITJerseyRestExpireProcedureInMemory extends AbstractRestGC {

  @NessieDbAdapter(storeWorker = TableCommitMetaStoreWorker.class)
  static DatabaseAdapter databaseAdapter;

  @RegisterExtension
  static NessieJaxRsExtension server = new NessieJaxRsExtension(() -> databaseAdapter);

  private static URI nessieUri;

  @BeforeAll
  static void setNessieUri(@NessieUri URI uri) {
    nessieUri = uri;
  }

  @Override
  @BeforeEach
  public void setUp() {
    init(nessieUri);
  }

  @TempDir static File LOCAL_DIR;

  static final String CATALOG_NAME = "nessie";
  static final String GC_BRANCH_NAME = "gcRef";
  static final String GC_OUTPUT_TABLE_NAME = "gc_results";
  static final String GC_SPARK_CATALOG = "org.projectnessie.gc.iceberg.NessieIcebergGcSparkCatalog";

  static final String TABLE_ONE = "table_1";
  static final String TABLE_TWO = "table_2";
  static final String TABLE_THREE = "table_3";

  private static final Schema icebergSchema =
      new Schema(
          Types.StructType.of(
                  required(1, "content_id", Types.StringType.get()),
                  required(2, "type", Types.StringType.get()),
                  required(3, "deleted_files_count", Types.IntegerType.get()))
              .fields());

  private static final StructType schema = SparkSchemaUtil.convert(icebergSchema);

  @Override
  protected SparkSession getSparkSession() {
    return ProcedureTestUtil.getSessionWithGcCatalog(
        getUri().toString(), LOCAL_DIR.toURI().toString(), GC_SPARK_CATALOG, "main");
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

      String cid1 = getContentId(prefix, branch1, TABLE_ONE);
      ProcedureTestUtil.dropBranch(sparkSession, CATALOG_NAME, branch1);

      Instant cutoffTime = Instant.now();

      performGcAndVerify(
          sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      List<Row> expectedExpiredFiles = new ArrayList<>();
      expectedExpiredFiles.add(createRow(cid1, "ICEBERG_MANIFESTLIST", 2));
      expectedExpiredFiles.add(createRow(cid1, "ICEBERG_MANIFEST", 2));
      expectedExpiredFiles.add(createRow(cid1, "DATA_FILE", 2));
      Dataset<Row> expiredDataset = createDataset(sparkSession, expectedExpiredFiles);

      performExpiry(prefix, sparkSession, expiredDataset, true);
      performExpiry(prefix, sparkSession, expiredDataset, false);
    }
  }

  private String getContentId(String prefix, String branch1, String tableName) {
    try {
      return getApi()
          .getContent()
          .key(ContentKey.of(prefix, tableName))
          .refName(branch1)
          .get()
          .get(ContentKey.of(prefix, tableName))
          .getId();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
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
      String cid3 = getContentId(prefix, branch3, TABLE_THREE);
      dropTable(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      dropTable(sparkSession, CATALOG_NAME, prefix, TABLE_THREE);

      Instant cutoffTime = Instant.now();

      useReference(sparkSession, CATALOG_NAME, branch1);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);

      useReference(sparkSession, CATALOG_NAME, branch3);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      performGcAndVerify(
          sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      List<Row> expectedExpiredFiles = new ArrayList<>();
      expectedExpiredFiles.add(createRow(cid3, "ICEBERG_MANIFESTLIST", 1));
      expectedExpiredFiles.add(createRow(cid3, "ICEBERG_MANIFEST", 1));
      expectedExpiredFiles.add(createRow(cid3, "DATA_FILE", 1));

      performExpiry(prefix, sparkSession, createDataset(sparkSession, expectedExpiredFiles), false);
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

      performGcAndVerify(
          sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      List<Row> expectedExpiredFiles = new ArrayList<>();
      String cid1 = getContentId(prefix, branch1, TABLE_TWO);
      expectedExpiredFiles.add(createRow(cid1, "ICEBERG_MANIFESTLIST", 2));

      performExpiry(prefix, sparkSession, createDataset(sparkSession, expectedExpiredFiles), false);
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

      performGcAndVerify(
          sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      List<Row> expectedExpiredFiles = new ArrayList<>();
      String cid1 = getContentId(prefix, branch1, TABLE_ONE);
      expectedExpiredFiles.add(createRow(cid1, "ICEBERG_MANIFESTLIST", 2));

      performExpiry(prefix, sparkSession, createDataset(sparkSession, expectedExpiredFiles), false);

      // identify the expired contents after checkpoint. It should return empty results
      performGcAndVerify(
          sparkSession, prefix, cutoffTime, Collections.emptyMap(), Collections.emptyList(), null);

      // clear the checkpoint point
      useReference(sparkSession, CATALOG_NAME, GC_BRANCH_NAME);
      ProcedureTestUtil.sql(
          sparkSession,
          "DELETE FROM %s WHERE %s = '%s' OR %s = '%s'",
          CATALOG_NAME + "." + prefix + "." + GC_OUTPUT_TABLE_NAME,
          "rowType",
          IdentifiedResultsRepo.RowType.CHECKPOINT.name(),
          "rowType",
          IdentifiedResultsRepo.RowType.CHECKPOINT_MARKER.name());

      // should collect the expired contents from the beginning of time.
      Dataset<Row> dataset =
          performGc(sparkSession, prefix, Instant.now(), Collections.emptyMap(), null);
      // identify branch and expire branch will have some expired contents.
      // Expire branch's commit hash is not visible outside as the reference is dropped after
      // expiry.
      // Hence, only validate original references.
      dataset = dataset.filter(dataset.col("referenceName").equalTo("checkpoint_1"));
      verify(dataset, expectedExpired, sparkSession, IdentifiedResultsRepo.getSchema());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"rewrite_data_files", "rewrite_manifests"})
  public void testRewrite(String type) {
    // ------  Time ---- | --- branch1 -----------------|
    //         t0        | Create branch                |
    //         t1        | TABLE_ONE : ID_1 (expired)   | // creates one new manifest
    //         t2        | TABLE_ONE : ID_2 (expired)   | // creates one new manifest
    //         t3        | TABLE_ONE : ID_3 [rewrite] (expired)| // creates N manifests
    //         t4        | TABLE_ONE : ID_4             | // creates one new manifest
    //         t5        |-- cut off time --------------|
    String prefix = "rewrite";
    String branch1 = prefix + "_1";
    try (SparkSession sparkSession =
        ProcedureTestUtil.getSessionWithGcCatalog(
            getUri().toString(), LOCAL_DIR.toURI().toString(), GC_SPARK_CATALOG, branch1)) {
      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch1, "main");
      useReference(sparkSession, CATALOG_NAME, branch1);
      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      useReference(sparkSession, CATALOG_NAME, branch1);

      String sqlText;
      if (type.equals("rewrite_data_files")) {
        sqlText =
            String.format(
                "CALL %s.system.rewrite_data_files(table => '%s.%s.%s', options => map('min-input-files','2'))",
                CATALOG_NAME, CATALOG_NAME, "rewrite", TABLE_ONE);
      } else {
        sqlText =
            String.format(
                "CALL %s.system.rewrite_manifests(table => '%s.%s.%s')",
                CATALOG_NAME, CATALOG_NAME, "rewrite", TABLE_ONE);
      }

      // After rewrite, rewritten-files-count should be 2 and added-files-count should be 1.
      assertThat(sparkSession.sql(sqlText).collectAsList())
          .hasSize(1)
          .extracting(row -> row.getInt(0), row -> row.getInt(1))
          .containsExactly(tuple(2, 1));

      List<Row> expectedExpired = new ArrayList<>();
      fillExpectedContents(Branch.of(branch1, null), 3, expectedExpired);

      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      Instant cutoffTime = Instant.now();

      performGcAndVerify(
          sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      List<Row> expectedExpiredFiles = new ArrayList<>();
      String cid1 = getContentId(prefix, branch1, TABLE_ONE);
      // Rewrite operation creates three manifests if rewrite_data_files operation
      // or one manifest if rewrite_manifests operation
      expectedExpiredFiles.add(createRow(cid1, "ICEBERG_MANIFESTLIST", 3));
      if (type.equals("rewrite_data_files")) {
        expectedExpiredFiles.add(createRow(cid1, "ICEBERG_MANIFEST", 4));
        // only compaction operation should allow expiring the old data files.
        expectedExpiredFiles.add(createRow(cid1, "DATA_FILE", 2));
      } else {
        expectedExpiredFiles.add(createRow(cid1, "ICEBERG_MANIFEST", 2));
      }
      performExpiry(prefix, sparkSession, createDataset(sparkSession, expectedExpiredFiles), false);
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

      performGcAndVerify(
          sparkSession, prefix, cutoffTime, Collections.emptyMap(), expectedExpired, null);

      List<Row> expectedExpiredFiles = new ArrayList<>();
      String cid1 = getContentId(prefix, branch1, TABLE_ONE);
      expectedExpiredFiles.add(createRow(cid1, "ICEBERG_MANIFESTLIST", 5));

      performExpiry(prefix, sparkSession, createDataset(sparkSession, expectedExpiredFiles), false);
    }
  }

  private void performExpiry(
      String prefix, SparkSession sparkSession, Dataset<Row> rows, Boolean dryRun) {
    Dataset<Row> output =
        sparkSession.sql(
            String.format(
                "CALL %s.%s.%s("
                    + "nessie_catalog_name => '%s', "
                    + "output_branch_name => '%s', "
                    + "output_table_identifier => '%s', "
                    + "nessie_client_configurations => map('%s','%s'), "
                    + "dry_run => %s)",
                CATALOG_NAME,
                NAMESPACE,
                ExpireContentsProcedure.PROCEDURE_NAME,
                //
                CATALOG_NAME,
                GC_BRANCH_NAME,
                prefix + "." + GC_OUTPUT_TABLE_NAME,
                CONF_NESSIE_URI,
                getUri().toString(),
                dryRun));
    verifyExpiry(output, rows, dryRun);
  }

  private void performGcAndVerify(
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

  private void verifyExpiry(Dataset<Row> actual, Dataset<Row> dfExpected, boolean dryRun) {
    Dataset<Row> dfActual =
        actual.select("content_id", "deleted_files_type", "deleted_files_count");
    // when both the dataframe is same, df.except() should return empty.
    assertThat(dfActual.count()).isEqualTo(dfExpected.count());
    assertThat(dfExpected.except(dfActual).collectAsList()).isEmpty();

    try {
      FileSystem localFs = FileSystem.getLocal(new Configuration());
      List<Row> deletedFilesList = actual.select("deleted_files_list").collectAsList();
      deletedFilesList.stream()
          .map(row -> row.getList(0))
          .forEach(
              files ->
                  files.forEach(
                      file -> {
                        try {
                          // verify whether the file exists are not based on dryRun configuration
                          assertThat(localFs.exists(new Path((String) file))).isEqualTo(dryRun);
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Row createRow(String contentID, String type, int count) {
    return RowFactory.create(contentID, type, count);
  }

  private static Dataset<Row> createDataset(SparkSession session, List<Row> rows) {
    return session.createDataFrame(rows, schema);
  }
}
