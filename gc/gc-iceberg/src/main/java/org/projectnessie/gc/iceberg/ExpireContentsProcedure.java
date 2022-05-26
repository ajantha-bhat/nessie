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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.procedures.BaseGcProcedure;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.projectnessie.gc.base.IdentifiedResultsRepo;

/**
 * Nessie GC procedure to expire unused snapshots, uses the information written by {@link
 * IdentifyExpiredContentsProcedure} via {@link org.projectnessie.gc.base.IdentifiedResultsRepo}.
 */
public class ExpireContentsProcedure extends BaseGcProcedure {

  public static final String PROCEDURE_NAME = "expire_contents";

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("nessie_catalog_name", DataTypes.StringType),
        ProcedureParameter.required("output_branch_name", DataTypes.StringType),
        ProcedureParameter.required("output_table_identifier", DataTypes.StringType),
        ProcedureParameter.required(
            "nessie_client_configurations",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        ProcedureParameter.optional("run_id", DataTypes.StringType),
        ProcedureParameter.optional("dry_run", DataTypes.BooleanType),
      };

  public static final String OUTPUT_CONTENT_ID = "content_id";
  public static final String OUTPUT_EXPIRED_DATA_FILES_TYPE = "deleted_files_type";
  public static final String OUTPUT_EXPIRED_DATA_FILES_COUNT = "deleted_files_count";
  public static final String OUTPUT_EXPIRED_FILES_LIST = "deleted_files_list";

  public static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(OUTPUT_CONTENT_ID, DataTypes.StringType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_DATA_FILES_TYPE, DataTypes.StringType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_DATA_FILES_COUNT, DataTypes.IntegerType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_FILES_LIST,
                DataTypes.createArrayType(DataTypes.StringType),
                true,
                Metadata.empty())
          });

  public enum ExpiredContentType {
    ICEBERG_MANIFEST,
    ICEBERG_MANIFESTLIST,
    DATA_FILE
  }

  public ExpireContentsProcedure(TableCatalog currentCatalog) {
    super(currentCatalog);
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
    return String.format(
        "Expires the Iceberg snapshots that are collected by '%s' procedure.",
        IdentifyExpiredContentsProcedure.PROCEDURE_NAME);
  }

  @Override
  public InternalRow[] call(InternalRow internalRow) {
    String gcCatalogName = internalRow.getString(0);
    String gcOutputBranchName = internalRow.getString(1);
    String gcOutputTableIdentifier = internalRow.getString(2);
    Map<String, String> nessieClientConfig = new HashMap<>();
    MapData map = internalRow.getMap(3);
    for (int i = 0; i < map.numElements(); i++) {
      nessieClientConfig.put(
          map.keyArray().getUTF8String(i).toString(), map.valueArray().getUTF8String(i).toString());
    }
    String runId = !internalRow.isNullAt(4) ? internalRow.getString(4) : null;
    boolean dryRun = !internalRow.isNullAt(5) && internalRow.getBoolean(5);

    IdentifiedResultsRepo identifiedResultsRepo =
        new IdentifiedResultsRepo(
            spark(), gcCatalogName, gcOutputBranchName, gcOutputTableIdentifier);

    runId = updateRunId(gcOutputTableIdentifier, runId, identifiedResultsRepo);
    FileIO fileIO = getFileIO(nessieClientConfig);
    Set<String> expiredManifests = getExpiredManifests(runId, identifiedResultsRepo, fileIO);

    Dataset<Row> expiredContents =
        getExpiredContents(runId, identifiedResultsRepo, expiredManifests, fileIO);

    expiredContents.persist();

    if (!dryRun) {
      expiredContents.foreach(
          row -> {
            List<String> files = row.getList(2);
            files.forEach(fileIO::deleteFile);
          });
    }

    List<InternalRow> outputRows = new ArrayList<>();
    List<Row> rows = expiredContents.collectAsList();

    expiredContents.unpersist();

    rows.forEach(
        row ->
            outputRows.add(
                GcProcedureUtil.internalRow(
                    row.getString(0), row.getString(1), row.getInt(3), row.getList(2))));

    if (!dryRun && !outputRows.isEmpty()) {
      Row markerRow = identifiedResultsRepo.createCheckPointMarkerRow(runId);
      identifiedResultsRepo.writeToOutputTable(Collections.singletonList(markerRow));
    }

    return outputRows.toArray(new InternalRow[0]);
  }

  private FileIO getFileIO(Map<String, String> nessieClientConfig) {
    Configuration config = spark().sessionState().newHadoopConf();
    String fileIOImpl = nessieClientConfig.get(CatalogProperties.FILE_IO_IMPL);
    return fileIOImpl == null
        ? new HadoopFileIO(config)
        : CatalogUtil.loadFileIO(fileIOImpl, nessieClientConfig, config);
  }

  private static Set<String> getExpiredManifests(
      String runId, IdentifiedResultsRepo identifiedResultsRepo, FileIO io) {
    Dataset<Row> rowDataset = identifiedResultsRepo.collectAllContentsAsDataSet(runId);
    Dataset<Row> dataset =
        rowDataset.withColumn(
            "manifestLocations", computeManifestsUDF(rowDataset.col("metadataLocation"), io));
    dataset =
        dataset
            .withColumn("manifestLocations", functions.explode(dataset.col("manifestLocations")))
            .select("manifestLocations", "isExpired");
    Dataset<Row> expired =
        dataset.filter("isExpired=true").dropDuplicates().select("manifestLocations");
    Dataset<Row> live =
        dataset.filter("isExpired=false").dropDuplicates().select("manifestLocations");
    Dataset<Row> unreachableManifests = expired.except(live);
    return unreachableManifests.select("manifestLocations").collectAsList().stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }

  private static Dataset<Row> getExpiredContents(
      String runId,
      IdentifiedResultsRepo identifiedResultsRepo,
      Set<String> expiredManifests,
      FileIO fileIO) {
    Dataset<Row> rowDataset = identifiedResultsRepo.collectExpiredContentsAsDataSet(runId);

    Dataset<Row> dataset =
        rowDataset.withColumn(
            "expiredFilesArray",
            computeExpiredFilesUDF(rowDataset.col("metadataLocation"), expiredManifests, fileIO));

    dataset =
        dataset
            .withColumn("expiredFilesWithType", functions.explode(dataset.col("expiredFilesArray")))
            .select("contentId", "expiredFilesWithType")
            .dropDuplicates();

    dataset =
        dataset
            .withColumn(
                "expiredFilesAndType",
                functions.split(functions.col("expiredFilesWithType"), "#", 2))
            .select(
                functions.col("contentId"),
                functions.col("expiredFilesAndType").getItem(0).as("Type"),
                functions.col("expiredFilesAndType").getItem(1).as("expiredFiles"));

    return dataset
        .groupBy("contentId", "Type")
        .agg(functions.collect_list("expiredFiles").as("expiredFilesList"))
        .withColumn("count", functions.size(functions.col("expiredFilesList")));
  }

  private static Column computeManifestsUDF(Column metadataLocation, FileIO fileIO) {
    return functions
        .udf(new ComputeManifestsUDF(fileIO), DataTypes.createArrayType(DataTypes.StringType))
        .apply(metadataLocation);
  }

  private static Column computeExpiredFilesUDF(
      Column metadataLocation, Set<String> expiredManifests, FileIO fileIO) {
    return functions
        .udf(
            new ComputeExpiredFilesUDF(fileIO, expiredManifests),
            DataTypes.createArrayType(DataTypes.StringType))
        .apply(metadataLocation);
  }

  private static String updateRunId(
      String gcOutputTableName, String runId, IdentifiedResultsRepo identifiedResultsRepo) {
    if (runId == null) {
      Optional<String> latestCompletedRunID = identifiedResultsRepo.getLatestCompletedRunID();
      if (!latestCompletedRunID.isPresent()) {
        throw new RuntimeException(
            String.format(
                "No runId present in gc output table : %s, please execute %s first",
                gcOutputTableName, IdentifyExpiredContentsProcedure.PROCEDURE_NAME));
      }
      runId = latestCompletedRunID.get();
    }
    return runId;
  }
}
