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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.spark.procedures.BaseGcProcedure;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.unsafe.types.UTF8String;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

/**
 * Base class for all Nessie GC related procedures.
 *
 * <p>Provides fields and accessors for common parameters, Nessie GC procedure related utility
 * functions.
 */
abstract class AbstractGcProcedure extends BaseGcProcedure {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGcProcedure.class);

  private final JavaSparkContext sparkContext;
  private int writerBatchSize;
  private String gcTable;
  private String catalog;
  private String branch;

  public AbstractGcProcedure(TableCatalog currentCatalog) {
    super(currentCatalog);
    this.sparkContext = JavaSparkContext.fromSparkContext(spark().sparkContext());
    this.catalog = currentCatalog.name();
    this.gcTable = IcebergGcRepo.DEFAULT_GC_IDENTIFY_RUNS;
    this.writerBatchSize = IcebergGcRepo.DEFAULT_BATCH_SIZE;
  }

  protected abstract String procedureName();

  protected String getCatalog() {
    return catalog;
  }

  protected void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  protected void setBranch(String branch) {
    this.branch = branch;
  }

  protected void setWriterBatchSize(int writerBatchSize) {
    this.writerBatchSize = writerBatchSize;
  }

  protected void setGcTable(String gcTable) {
    this.gcTable = gcTable;
  }

  static Map<String, String> catalogConfWithRef(SparkSession spark, String catalog, String branch) {
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

  protected NessieCatalog createNessieGcCatalog() {
    NessieCatalog nessieCatalog =
        (NessieCatalog)
            CatalogUtil.loadCatalog(
                NessieCatalog.class.getName(),
                catalog,
                catalogConfWithRef(spark(), catalog, branch),
                sparkContext.hadoopConfiguration());

    boolean ok = false;
    try {
      String branchName =
          branch != null
              ? branch
              : spark().conf().get(String.format("spark.sql.catalog.%s.ref", catalog));
      ensureBranchExists(nessieCatalog, branchName, false, "identified live snapshots");
      ok = true;
    } finally {
      if (!ok) {
        nessieCatalog.close();
      }
    }

    return nessieCatalog;
  }

  protected void ensureBranchExists(
      NessieCatalog nessieCatalog,
      String branch,
      boolean failForDefault,
      String branchDescription) {
    String defaultBranch;
    try {
      defaultBranch = nessieCatalog.api().getDefaultBranch().getName();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException("Failed to retrieve default branch name from Nessie", e);
    }

    if (branch.equals(defaultBranch)) {
      if (failForDefault) {
        throw new RuntimeException(
            String.format(
                "Nessie %s operation is configured to the Nessie default branch '%s' for %s. "
                    + "It is required to use a different branch for %s.",
                procedureName(), branch, branchDescription, branchDescription));
      } else {
        LOGGER.warn(
            "Nessie {} operation is configured to the Nessie default branch '{}' for {}. "
                + "It is recommended to use a different branch for {}.",
            procedureName(),
            branch,
            branchDescription,
            branchDescription);
      }
    }

    try {
      nessieCatalog
          .api()
          .createReference()
          .reference(Branch.of(branch, null))
          .sourceRefName(defaultBranch)
          .create();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format("Failed to create branch '%s' for %s.", branch, branchDescription), e);
    } catch (NessieConflictException e) {
      // already exists
    }
  }

  protected IcebergGcRepo openRepo() {
    TableIdentifier tableIdent = TableIdentifier.parse(gcTable);
    String name = tableIdent.name();
    if (branch != null) {
      name = String.format("`%s@%s`", name, branch);
    }
    // TODO
    return new IcebergGcRepo(spark(), writerBatchSize, catalog, tableIdent);
  }

  static InternalRow internalRow(Object... columns) {
    Seq<Object> seq =
        JavaConverters.collectionAsScalaIterable(
                Arrays.stream(columns)
                    .map(o -> o instanceof String ? UTF8String.fromString((String) o) : o)
                    .collect(Collectors.toList()))
            .toSeq();
    return InternalRow.fromSeq(seq);
  }

  private static Object toSparkObject(Object o) {
    if (o instanceof String) {
      return UTF8String.fromString((String) o);
    }
    if (o instanceof List) {
      List<?> converted =
          ((List<?>) o)
              .stream().map(AbstractGcProcedure::toSparkObject).collect(Collectors.toList());
      return ArrayData.toArrayData(
          JavaConverters.collectionAsScalaIterable(converted).toArray(ClassTag.Any()));
    }
    return o;
  }
}
