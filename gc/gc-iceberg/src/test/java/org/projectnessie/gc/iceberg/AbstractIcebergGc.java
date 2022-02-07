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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.stubbing.OngoingStubbing;
import org.projectnessie.api.http.HttpConfigApi;
import org.projectnessie.api.http.HttpContentApi;
import org.projectnessie.api.http.HttpTreeApi;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.client.http.v1api.HttpApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.rest.RestConfigResource;
import org.projectnessie.services.rest.RestContentResource;
import org.projectnessie.services.rest.RestTreeResource;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.store.PersistVersionStore;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.threeten.extra.MutableClock;

@ExtendWith(DatabaseAdapterExtension.class)
abstract class AbstractIcebergGc {

  static final String SPARK_CATALOG = "spark_catalog";
  static final String DEFAULT_BRANCH = "main";
  static final String DEFAULT_CATALOG = "default_iceberg";
  static final String GC_CATALOG = "gc_catalog";
  static final String GC_IDENTIFY_BRANCH = "gc_identify";
  static final String GC_EXPIRE_BRANCH = "gc_expire";

  static final String BRANCH_A = "branch_a";
  static final String BRANCH_B = "branch_b";

  static final String TABLE_1 = "foo.table_1";
  static final String TABLE_2 = "foo.table_2";
  static final String TABLE_3 = "foo.table_3";
  static final String TABLE_2_RENAMED = TABLE_2 + "_renamed";

  static final String T1 = DEFAULT_CATALOG + "." + TABLE_1;
  static final String T2 = DEFAULT_CATALOG + "." + TABLE_2;
  static final String T3 = DEFAULT_CATALOG + "." + TABLE_3;
  static final String T2_RENAMED = T2 + "_renamed";

  @NessieDbAdapter protected static DatabaseAdapter databaseAdapter;

  static final MutableClock TEST_CLOCK = MutableClock.epochUTC();

  static final TableCommitMetaStoreWorker STORE_WORKER = new TableCommitMetaStoreWorker();
  static final Instant NOT_LIVE = Instant.ofEpochSecond(500);

  private static final String NESSIE_ENDPOINT = "http://localhost:42/api/v1";
  @TempDir static File LOCAL_DIR;

  private static SparkSession sparkMain;

  @BeforeAll
  static void create() {
    TEST_CLOCK.setInstant(Instant.EPOCH);

    SparkConf conf = new SparkConf();
    String ssc = "spark.sql.catalog.";
    conf.set(ssc + SPARK_CATALOG + ".uri", NESSIE_ENDPOINT)
        .set(ssc + SPARK_CATALOG + ".ref", DEFAULT_BRANCH)
        .set(ssc + SPARK_CATALOG + ".warehouse", LOCAL_DIR.toURI().toString())
        .set(ssc + SPARK_CATALOG + ".catalog-impl", NessieCatalog.class.getName())
        .set(ssc + SPARK_CATALOG, NessieIcebergGcSparkSessionCatalog.class.getName())
        .set(ssc + DEFAULT_CATALOG + ".uri", NESSIE_ENDPOINT)
        .set(ssc + DEFAULT_CATALOG + ".ref", DEFAULT_BRANCH)
        .set(ssc + DEFAULT_CATALOG + ".warehouse", LOCAL_DIR.toURI().toString())
        .set(ssc + DEFAULT_CATALOG + ".catalog-impl", NessieCatalog.class.getName())
        .set(ssc + DEFAULT_CATALOG, NessieIcebergGcSparkCatalog.class.getName())
        .set(ssc + GC_CATALOG + ".uri", NESSIE_ENDPOINT)
        .set(ssc + GC_CATALOG + ".ref", GC_IDENTIFY_BRANCH)
        .set(ssc + GC_CATALOG + ".warehouse", LOCAL_DIR.toURI().toString())
        .set(ssc + GC_CATALOG + ".catalog-impl", NessieCatalog.class.getName())
        .set(ssc + GC_CATALOG, NessieIcebergGcSparkCatalog.class.getName())
        .set("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
        .set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "static");
    SparkSession spark =
        SparkSession.builder()
            .appName("test-nessie-gc-iceberg")
            .config(conf)
            .master("local[2]")
            .getOrCreate();

    sparkMain = spark.newSession();
  }

  static List<IcebergGcScenario> icebergGcScenarios() {
    // return Arrays.asList(
    //     differentSchemas(),
    //     repeatedIdentifyExpireRuns(),
    //     singleBranchSimple(),
    //     twoBranchSeparate(),
    //     twoBranchShared());
    return Arrays.asList(
      singleBranchSimple());
  }

  private static IcebergGcScenario differentSchemas() {
    return new IcebergGcScenario("differentSchemas")
        .setClock(NOT_LIVE.minusSeconds(10))
        .againstSpark(
            sparkSession -> {
              sparkSession.sql(
                  "CREATE TABLE "
                      + T1
                      + " (col_a STRING, col_b STRING, col_c STRING) USING iceberg");
              sparkSession.sql("INSERT INTO " + T1 + " VALUES ('foo', 'bar', 'baz')");
            })
        .createBranch(BRANCH_A, "main")
        .createBranch(BRANCH_B, "main")
        //
        .changeRef(DEFAULT_CATALOG, BRANCH_A)
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("ALTER TABLE " + T1 + " ADD COLUMN add_a STRING");
              sparkSession.sql("INSERT INTO " + T1 + " VALUES ('a', 'b', 'c', 'add')");
            })
        //
        .changeRef(DEFAULT_CATALOG, BRANCH_B)
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("ALTER TABLE " + T1 + " ADD COLUMN add_b BIGINT");
              sparkSession.sql("ALTER TABLE " + T1 + " RENAME COLUMN col_c TO col_c_renamed");
              sparkSession.sql("INSERT INTO " + T1 + " VALUES ('a', 'b', 'c', 123)");
            })
        //
        .changeRef(DEFAULT_CATALOG, BRANCH_A)
        .againstSpark(
            sparkSession ->
                sparkSession.sql("INSERT INTO " + T1 + " VALUES ('a2', 'b2', 'c2', 'add2')"))
        //
        .changeRef(DEFAULT_CATALOG, BRANCH_B)
        .againstSpark(
            sparkSession ->
                sparkSession.sql("INSERT INTO " + T1 + " VALUES ('a2', 'b2', 'c2', 42)"))
        //
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH);
  }

  private static IcebergGcScenario repeatedIdentifyExpireRuns() {
    return new IcebergGcScenario("repeatedIdentifyExpireRuns")
        .setClock(NOT_LIVE.minusSeconds(10))
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("CREATE TABLE " + T1 + " (some_text STRING) USING iceberg");
              sparkSession.sql("INSERT INTO " + T1 + " VALUES ('foo')");
            })
        .setClock(NOT_LIVE.plusSeconds(10))
        .againstSpark(sparkSession -> sparkSession.sql("INSERT INTO " + T1 + " VALUES ('bar')"))
        .expectCollect(DEFAULT_CATALOG, TABLE_1, 1, 1)
        //
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH)
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH);
  }

  private static IcebergGcScenario twoBranchShared() {
    return new IcebergGcScenario("twoBranchesShared")
        .createBranch(BRANCH_A, "main")
        .createBranch(BRANCH_B, "main")
        //
        .setClock(NOT_LIVE.minusSeconds(10))
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("CREATE TABLE " + T1 + " (some_text STRING) USING iceberg");
              sparkSession.sql("INSERT INTO " + T1 + " VALUES ('foo')");
            })
        .expectCollect(DEFAULT_CATALOG, TABLE_1, 0, 1)
        //
        .changeRef(DEFAULT_CATALOG, BRANCH_A)
        .setClock(NOT_LIVE.minusSeconds(10))
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("CREATE TABLE " + T2 + " (some_text STRING) USING iceberg");
              sparkSession.sql("INSERT INTO " + T2 + " VALUES ('foo')");
            })
        .setClock(NOT_LIVE.plusSeconds(1))
        .againstSpark(sparkSession -> sparkSession.sql("INSERT INTO " + T2 + " VALUES ('bvar')"))
        .expectCollect(DEFAULT_CATALOG, TABLE_2, 1, 1)
        //
        .changeRef(DEFAULT_CATALOG, BRANCH_B)
        .setClock(NOT_LIVE.minusSeconds(10))
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("CREATE TABLE " + T3 + " (some_text STRING) USING iceberg");
              sparkSession.sql("INSERT INTO " + T3 + " VALUES ('foo')");
              sparkSession.sql("ALTER TABLE " + T3 + " RENAME TO " + T2);
            })
        .setClock(NOT_LIVE.plusSeconds(1))
        .againstSpark(sparkSession -> sparkSession.sql("INSERT INTO " + T2 + " VALUES ('bvar')"))
        .expectCollect(DEFAULT_CATALOG, TABLE_2, 1, 1)
        //
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH);
  }

  private static IcebergGcScenario twoBranchSeparate() {
    return new IcebergGcScenario("twoBranchesSeparate")
        .createBranch(BRANCH_A, "main")
        .createBranch(BRANCH_B, "main")
        //
        .changeRef(DEFAULT_CATALOG, BRANCH_A)
        .setClock(NOT_LIVE.minusSeconds(10))
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("CREATE TABLE " + T1 + " (some_text STRING) USING iceberg");
              sparkSession.sql("INSERT INTO " + T1 + " VALUES ('foo')");
            })
        .setClock(NOT_LIVE.plusSeconds(10))
        .againstSpark(sparkSession -> sparkSession.sql("INSERT INTO " + T1 + " VALUES ('bar')"))
        .expectCollect(DEFAULT_CATALOG, TABLE_1, 1, 1)
        //
        .changeRef(DEFAULT_CATALOG, BRANCH_B)
        .setClock(NOT_LIVE.minusSeconds(10))
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("CREATE TABLE " + T1 + " (some_text STRING) USING iceberg");
              sparkSession.sql("INSERT INTO " + T1 + " VALUES ('foo_2')");
            })
        .setClock(NOT_LIVE.plusSeconds(10))
        .againstSpark(sparkSession -> sparkSession.sql("INSERT INTO " + T1 + " VALUES ('bar_2')"))
        .expectCollect(DEFAULT_CATALOG, TABLE_1, 1, 1)
        //
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH);
  }

  private static IcebergGcScenario singleBranchSimple() {
    return new IcebergGcScenario("singleBranchSimple")
        .againstSpark(
            sparkSession -> {
              sparkSession.sql(
                  "CREATE TABLE IF NOT EXISTS " + T1 + " " + "(some_text STRING) USING iceberg");
              sparkSession.sql(
                  "CREATE TABLE IF NOT EXISTS " + T2 + " (some_string STRING) USING iceberg");
              sparkSession.sql("INSERT INTO " + T2 + " VALUES ('foo')");
            })
        .setClock(NOT_LIVE.minusSeconds(3))
        .againstSpark(
            sparkSession -> sparkSession.sql("INSERT INTO " + T1 + " VALUES ('hello'), ('world')"))
        .expectCollect(DEFAULT_CATALOG, TABLE_1, 0, 1)
        .setClock(NOT_LIVE.minusSeconds(2))
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("DROP TABLE " + T1);
              sparkSession.sql("INSERT INTO " + T2 + " VALUES ('bar')");
            })
        .setClock(NOT_LIVE.minusSeconds(1))
        .againstSpark(
            sparkSession -> {
              sparkSession.sql("ALTER TABLE " + T2 + " RENAME TO " + T2_RENAMED);
              sparkSession.sql("INSERT INTO " + T2_RENAMED + " VALUES ('baz')");
            })
        .setClock(NOT_LIVE.plusSeconds(10))
        .againstSpark(
            sparkSession -> sparkSession.sql("INSERT INTO " + T2_RENAMED + " VALUES ('live')"))
        .expectCollect(DEFAULT_CATALOG, TABLE_2_RENAMED, 1, 3)
        .againstSpark(sparkSession -> sparkSession.sql("DROP TABLE " + T2_RENAMED))
        //
        .identifyProcedure(GC_CATALOG, NOT_LIVE)
        .expireProcedure(GC_CATALOG, GC_EXPIRE_BRANCH);
  }

  @ParameterizedTest
  @MethodSource("icebergGcScenarios")
  void icebergGcTest(IcebergGcScenario test) {
    HttpApiV1 api = getEmbeddedNessieApi();

    try {
      api.createReference()
          .sourceRefName(DEFAULT_BRANCH)
          .reference(Branch.of(GC_IDENTIFY_BRANCH, null))
          .create();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    } catch (NessieConflictException e) {
      // aka: branch to create does not exist
    }

    // Mock HttpClientBuilder to return the 'api' instance constructed above, so we can use our
    // local instances of HttpApiV1, VersionStore, DatabaseAdapter - and use constant timestamps
    // (no Thread.sleep() et al).
    HttpClientBuilder httpClientBuilderMock = mock(HttpClientBuilder.class);
    try (MockedStatic<HttpClientBuilder> clientStatic = mockStatic(HttpClientBuilder.class)) {
      mockReturn(clientStatic.when(HttpClientBuilder::builder), httpClientBuilderMock);
      mockReturn(when(httpClientBuilderMock.fromConfig(any())), httpClientBuilderMock);
      mockReturn(when(httpClientBuilderMock.build(any())), api);

      IcebergGcScenario.initSparkCatalogPlugin(sparkMain, SPARK_CATALOG, DEFAULT_BRANCH);
      IcebergGcScenario.initSparkCatalogPlugin(sparkMain, DEFAULT_CATALOG, DEFAULT_BRANCH);
      IcebergGcScenario.initSparkCatalogPlugin(sparkMain, GC_CATALOG, GC_IDENTIFY_BRANCH);

      // `test.runScenario` is where the assertions happen
      test.runScenario(sparkMain, api);
    }
  }

  @NotNull
  public static HttpApiV1 getEmbeddedNessieApi() {
    PersistVersionStore<Content, CommitMeta, Type> versionStore =
        new PersistVersionStore<>(databaseAdapter, STORE_WORKER);
    ServerConfig serverConfig =
        new ServerConfig() {
          @Override
          public String getDefaultBranch() {
            return DEFAULT_BRANCH;
          }

          @Override
          public boolean sendStacktraceToClient() {
            return false;
          }
        };

    final Authorizer authorizer = context -> AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER;
    HttpTreeApi treeApi = new RestTreeResource(serverConfig, versionStore,authorizer);
    HttpContentApi contentApi = new RestContentResource(serverConfig, versionStore, authorizer);
    HttpConfigApi configApi = new RestConfigResource(serverConfig);
    return new HttpApiV1(new NessieApiClient(configApi, treeApi, contentApi, null, null));
  }

  /**
   * {@link OngoingStubbing#thenReturn(Object)} returns the passed object for one invocation, so
   * let's add a ton of returns, so the test (hopefully) never runs out of responses.
   */
  static void mockReturn(OngoingStubbing<Object> when, Object r) {
    Object[] manyReturns = new Object[100];
    Arrays.fill(manyReturns, r);
    when.thenReturn(r, manyReturns);
  }
}
