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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.gc.iceberg.GcProcedureUtil.NAMESPACE;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.commit;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.createTable;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.dropTable;
import static org.projectnessie.gc.iceberg.ProcedureTestUtil.useReference;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.gc.base.AbstractRestGC;
import org.projectnessie.model.Branch;

public abstract class AbstractExpireProcedure extends AbstractRestGC {

  @TempDir static File LOCAL_DIR;

  static final String CATALOG_NAME = "nessie";
  static final String GC_BRANCH_NAME = "gcRef";
  static final String GC_TABLE = "gc_results";

  static final String TABLE_ONE = "table_1";
  static final String TABLE_TWO = "table_2";
  static final String TABLE_THREE = "table_3";
  static final String TABLE_TWO_RENAMED = "table_2_renamed";

  @Override
  protected SparkSession getSparkSession(String uri) {
    return ProcedureTestUtil.getSessionWithGcCatalog(uri, LOCAL_DIR.toURI().toString());
  }

  @Override
  protected void performGc(
      String prefix,
      Instant cutoffTimeStamp,
      Map<String, Instant> cutOffTimeStampPerRef,
      List<Row> expectedDataSet,
      boolean disableCommitProtection,
      Instant deadReferenceCutoffTime) {
    try (SparkSession session = getSparkSession(getUri().toString())) {
      ProcedureTestUtil.performGcWithProcedure(
          session,
          CATALOG_NAME,
          GC_BRANCH_NAME,
          prefix + "." + GC_TABLE,
          getUri().toString(),
          cutoffTimeStamp,
          expectedDataSet,
          disableCommitProtection,
          deadReferenceCutoffTime,
          cutOffTimeStampPerRef);
    }
  }

  @Test
  public void testEndToEnd() {
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
    String prefix = "expire_multiRefMultipleSharedTables";
    String branch1 = prefix + "_1";
    String branch2 = prefix + "_2";
    String branch3 = prefix + "_3";
    try (SparkSession sparkSession = getSparkSession(getUri().toString())) {
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
      // fill expected expired content
      List<Row> expectedExpired = new ArrayList<>();
      try {
        fillExpectedContents(Branch.of(branch3, null), 1, expectedExpired);
      } catch (NessieNotFoundException e) {
        throw new RuntimeException(e);
      }

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

      ProcedureTestUtil.performGcWithProcedure(
          sparkSession,
          CATALOG_NAME,
          GC_BRANCH_NAME,
          prefix + "." + GC_TABLE,
          getUri().toString(),
          cutoffTime,
          expectedExpired,
          true,
          null,
          Collections.emptyMap());

      Dataset<Row> output =
          sparkSession.sql(
              String.format(
                  "CALL %s.%s.%s("
                      + "expire_procedure_reference_name => '%s', "
                      + "nessie_catalog_name => '%s', "
                      + "output_branch_name => '%s', "
                      + "output_table_identifier => '%s', "
                      + "nessie_client_configurations => map('%s','%s'))",
                  CATALOG_NAME,
                  NAMESPACE,
                  ExpireSnapshotsProcedure.PROCEDURE_NAME,
                  //
                  "someExpireRef",
                  CATALOG_NAME,
                  GC_BRANCH_NAME,
                  prefix + "." + GC_TABLE,
                  CONF_NESSIE_URI,
                  getUri().toString()));

      // TODO: add validation
      output.show(false);
    }
  }

  @Test
  public void testMoreCommits() {
    // ------  Time ---- | --- branch1 ---------- ----| ---- branch2 -------------------- |
    //         t0        | create branch              |                                   |
    //         t1        | TABLE_ONE : ID_1           | {TABLE_ONE : ID_1}                |  -- expired
    //         t1        | TABLE_ONE : ID_2           | {TABLE_ONE : ID_2}                |  -- expired
    //         t1        | TABLE_ONE : ID_3           | {TABLE_ONE : ID_3}                |  -- expired
    //         t1        | TABLE_TWO : ID_1           | {TABLE_TWO : ID_1}                |  -- expired
    //         t2        |                            |  create branch                    |
    //         t3        | TABLE_ONE : ID_4 (expired) |                                   |
    //         t3        | TABLE_ONE : ID_5           |                                   |
    //         t4        |                            | TABLE_ONE : ID_6 (expired)        |
    //         t5        |                            | TABLE_ONE : ID_7                  |
    //         t6        |TABLE_TWO : ID_2 (expired)  |                                   |
    //         t6        |TABLE_TWO : ID_3            |                                   |
    //         t7        |                            |TABLE_TWO : ID_4 (expired)         |
    //         t7        |                            |TABLE_TWO : ID_5                   |
    //         t8        |                            |                                   |
    //         t9        |                            |                                   |
    //         t10       |-- cut off time -- -------- |-- cut off time -------------------|

    String prefix = "expire_multiRefMultipleSharedTables";
    String branch1 = prefix + "_1";
    String branch2 = prefix + "_2";
    try (SparkSession sparkSession = getSparkSession(getUri().toString())) {

      // fill expected expired content
      List<Row> expectedExpired = new ArrayList<>();

      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch1, "main");
      useReference(sparkSession, CATALOG_NAME, branch1);
      createTable(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      //
      // createTable(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      // commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      //
      ProcedureTestUtil.createBranch(sparkSession, CATALOG_NAME, branch2, branch1);
      //
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      try {
        fillExpectedContents(Branch.of(branch1, null), 5, expectedExpired);
      } catch (NessieNotFoundException e) {
        throw new RuntimeException(e);
      }
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      useReference(sparkSession, CATALOG_NAME, branch2);
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);
      try {
        fillExpectedContents(Branch.of(branch2, null), 5, expectedExpired);
      } catch (NessieNotFoundException e) {
        throw new RuntimeException(e);
      }
      commit(sparkSession, CATALOG_NAME, prefix, TABLE_ONE);

      useReference(sparkSession, CATALOG_NAME, branch1);
      // commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      // try {
      //   fillExpectedContents(Branch.of(branch1, null), 1, expectedExpired);
      // } catch (NessieNotFoundException e) {
      //   throw new RuntimeException(e);
      // }
      // commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);

      // useReference(sparkSession, CATALOG_NAME, branch2);
      // commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);
      // try {
      //   fillExpectedContents(Branch.of(branch2, null), 1, expectedExpired);
      // } catch (NessieNotFoundException e) {
      //   throw new RuntimeException(e);
      // }
      // commit(sparkSession, CATALOG_NAME, prefix, TABLE_TWO);

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Instant cutoffTime = Instant.now();

      ProcedureTestUtil.performGcWithProcedure(
        sparkSession,
        CATALOG_NAME,
        GC_BRANCH_NAME,
        prefix + "." + GC_TABLE,
        getUri().toString(),
        cutoffTime,
        expectedExpired,
        true,
        null,
        Collections.emptyMap());

      Dataset<Row> output =
        sparkSession.sql(
          String.format(
            "CALL %s.%s.%s("
              + "expire_procedure_reference_name => '%s', "
              + "nessie_catalog_name => '%s', "
              + "output_branch_name => '%s', "
              + "output_table_identifier => '%s', "
              + "nessie_client_configurations => map('%s','%s'))",
            CATALOG_NAME,
            NAMESPACE,
            ExpireSnapshotsProcedure.PROCEDURE_NAME,
            //
            "someExpireRef",
            CATALOG_NAME,
            GC_BRANCH_NAME,
            prefix + "." + GC_TABLE,
            CONF_NESSIE_URI,
            getUri().toString()));

      // TODO: add validation
      output.show(false);
    }
  }
}
