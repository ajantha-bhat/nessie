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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.procedures.BaseGcProcedure;
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
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.gc.base.GCCheckPointRepo;
import org.projectnessie.gc.base.GCUtil;
import org.projectnessie.gc.base.IdentifiedResultsRepo;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

/**
 * Nessie GC procedure to expire unused snapshots, uses the information written by {@link
 * IdentifyExpiredSnapshotsProcedure} via {@link org.projectnessie.gc.base.IdentifiedResultsRepo}.
 */
public class ExpireSnapshotsProcedure extends BaseGcProcedure {

  public static final String PROCEDURE_NAME = "expire_snapshots";
  private static final String EXPIRE_NAMESPACE = "expire_namespace";

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("expire_procedure_branch_name", DataTypes.StringType),
        ProcedureParameter.required("nessie_catalog_name", DataTypes.StringType),
        ProcedureParameter.required("output_branch_name", DataTypes.StringType),
        ProcedureParameter.required("output_table_identifier", DataTypes.StringType),
        ProcedureParameter.required("checkpoint_table_identifier", DataTypes.StringType),
        ProcedureParameter.required(
            "nessie_client_configurations",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        ProcedureParameter.optional("run_id", DataTypes.StringType),
        ProcedureParameter.optional("dry_run", DataTypes.BooleanType),
      };

  public static final String OUTPUT_CONTENT_ID = "content_id";
  public static final String OUTPUT_EXPIRED_DATA_FILES_COUNT = "deleted_data_files_count";
  public static final String OUTPUT_EXPIRED_MANIFEST_LISTS_COUNT = "deleted_manifest_lists_count";
  public static final String OUTPUT_EXPIRED_MANIFESTS_COUNT = "deleted_manifests_count";
  public static final String OUTPUT_EXPIRED_FILES_LIST = "deleted_files_list";
  public static final String OUTPUT_SNAPSHOT_IDS = "snapshot_ids";

  public static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(OUTPUT_CONTENT_ID, DataTypes.StringType, true, Metadata.empty()),
            new StructField(
                OUTPUT_SNAPSHOT_IDS,
                DataTypes.createArrayType(DataTypes.LongType),
                true,
                Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_DATA_FILES_COUNT, DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_MANIFEST_LISTS_COUNT, DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_MANIFESTS_COUNT, DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_FILES_LIST,
                DataTypes.createArrayType(DataTypes.StringType),
                true,
                Metadata.empty())
          });

  public ExpireSnapshotsProcedure(TableCatalog currentCatalog) {
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
        IdentifyExpiredSnapshotsProcedure.PROCEDURE_NAME);
  }

  @Override
  public InternalRow[] call(InternalRow internalRow) {
    String expiryBranchName = internalRow.getString(0);
    String gcCatalogName = internalRow.getString(1);
    String gcOutputBranchName = internalRow.getString(2);
    String gcOutputTableIdentifier = internalRow.getString(3);
    String gcCheckPointTableIdentifier = internalRow.getString(4);
    Map<String, String> nessieClientConfig = new HashMap<>();
    MapData map = internalRow.getMap(5);
    for (int i = 0; i < map.numElements(); i++) {
      nessieClientConfig.put(
          map.keyArray().getUTF8String(i).toString(), map.valueArray().getUTF8String(i).toString());
    }
    String runId = !internalRow.isNullAt(6) ? internalRow.getString(6) : null;
    boolean dryRun = !internalRow.isNullAt(7) && internalRow.getBoolean(7);

    IdentifiedResultsRepo identifiedResultsRepo =
        new IdentifiedResultsRepo(
            spark(), gcCatalogName, gcOutputBranchName, gcOutputTableIdentifier);

    runId = updateRunId(gcOutputTableIdentifier, runId, identifiedResultsRepo);

    Map<String, List<Long>> expiredSnapshotsPerContentId =
        getExpiredSnapshotsPerContentId(runId, identifiedResultsRepo);

    Map<String, List<String>> metadataLocationsPerContentId =
        getMetadataLocationsPerContentId(runId, identifiedResultsRepo);

    List<InternalRow> outputRows = new ArrayList<>();
    try (NessieApiV1 api = GCUtil.getApi(nessieClientConfig)) {
      createGCBranch(expiryBranchName, api);
      // get Nessie catalog
      Catalog nessieCatalog = GCUtil.loadNessieCatalog(spark(), gcCatalogName, expiryBranchName);
      String finalRunId = runId;
      expiredSnapshotsPerContentId
          .entrySet()
          .forEach(
              entry -> {
                // Expiry Algorithm:
                // A. For each content id, fetch ANY one of the content.
                // B. Use the newly created expiry branch. On this branch,
                // C. Do a single PUT operation with a fetched content from step-A and
                //    a dummy ContentKey.
                // D. Collect the list of all the expired snapshot for this content id.
                // E. Fetch this table using dummy ContentKey on expiry branch. For this table,
                // F. call expire_snapshot action from Iceberg by passing the list of expired
                //    snapshots.
                // G. Once all the content ids are handled, drop this branch.
                //    (Else it may impact the next GC operations as some contents are still referred
                // in
                //    this branch)
                String contentId = entry.getKey();
                addOneContentToExpireProcedureBranch(
                    expiryBranchName, identifiedResultsRepo, api, finalRunId, contentId);
                Table table =
                    nessieCatalog.loadTable(
                        TableIdentifier.parse(EXPIRE_NAMESPACE + "." + contentId));

                // TODO: Remove after https://github.com/apache/iceberg/pull/4509
                updateMetadataLocation((BaseTable) table);

                Set<Snapshot> snapshotSet =
                    new TreeSet<>((o1, o2) -> (int) (o1.timestampMillis() - o2.timestampMillis()));
                Set<String> processedMetadataFiles = new HashSet<>();
                List<String> metadataLocations =
                    new ArrayList<>(metadataLocationsPerContentId.get(contentId));

                metadataLocations.sort(Collections.reverseOrder());

                metadataLocations.forEach(
                    metadataLocation -> {
                      if (!processedMetadataFiles.contains(metadataLocation)) {
                        TableMetadata metadata =
                            TableMetadataParser.read(table.io(), metadataLocation);
                        snapshotSet.addAll(metadata.snapshots());
                        List<String> files =
                            metadata.previousFiles().stream()
                                .map(TableMetadata.MetadataLogEntry::file)
                                .collect(Collectors.toList());
                        processedMetadataFiles.addAll(files);
                      }
                    });

                // create a dummy table metadata which has all the snapshots for a content from all
                // the references.
                TableMetadata base = ((BaseTable) table).operations().current();
                base = TableMetadata.buildFrom(base).build();
                try {
                  FieldUtils.writeField(base, "previousFiles", Collections.emptyList(), true);
                  FieldUtils.writeField(base, "snapshotLog", Collections.emptyList(), true);
                } catch (IllegalAccessException e) {
                  throw new RuntimeException(e);
                }
                List<Snapshot> baseSnapshots = base.snapshots();
                TableMetadata.Builder builder =
                    TableMetadata.buildFrom(base).removeSnapshots(baseSnapshots);
                snapshotSet.forEach(builder::addSnapshot);
                ((BaseTable) table).operations().commit(base, builder.build());

                List<String> filesToBeDeleted = new ArrayList<>();
                // Call Iceberg expire_snapshots action.
                ExpireSnapshots.Result result =
                    executeExpireSnapshotsPerTable(dryRun, entry, filesToBeDeleted, table);
                // One content id result is mapped to one output row of this procedure.
                InternalRow outputRow =
                    GcProcedureUtil.internalRow(
                        contentId,
                        entry.getValue(),
                        result.deletedDataFilesCount(),
                        result.deletedManifestListsCount(),
                        result.deletedManifestsCount(),
                        filesToBeDeleted);
                outputRows.add(outputRow);
              });

      dropExpireProcedureBranch(expiryBranchName, api);

      GCCheckPointRepo gcCheckPointRepo =
          new GCCheckPointRepo(
              spark(), gcCatalogName, gcOutputBranchName, gcCheckPointTableIdentifier);
      Row markerRow = gcCheckPointRepo.createMarkerRow(runId);
      gcCheckPointRepo.writeToOutputTable(Collections.singletonList(markerRow));
    }
    return outputRows.toArray(new InternalRow[0]);
  }

  private static void addOneContentToExpireProcedureBranch(
      String expiryBranchName,
      IdentifiedResultsRepo identifiedResultsRepo,
      NessieApiV1 api,
      String finalRunId,
      String contentId) {
    String commitHash =
        identifiedResultsRepo.getAnyCommitHashForContentId(contentId, finalRunId).get();
    Content content = getContentFromCommitHash(api, commitHash, contentId);
    // keep the content key same as content id with dummy implicit namespace
    // EXPIRE_NAMESPACE
    String headCommitHash = getHeadCommitHash(api, expiryBranchName);
    CommitMeta commitMeta = CommitMeta.builder().message("putting the contents for GC").build();
    try {
      api.commitMultipleOperations()
          .branch(Branch.of(expiryBranchName, headCommitHash))
          .commitMeta(commitMeta)
          .operation(Operation.Put.of(ContentKey.of(EXPIRE_NAMESPACE, contentId), content, content))
          .commit();
    } catch (NessieNotFoundException | NessieConflictException e) {
      throw new RuntimeException(e);
    }
  }

  private static ExpireSnapshots.Result executeExpireSnapshotsPerTable(
      boolean dryRun,
      Map.Entry<String, List<Long>> entry,
      List<String> filesToBeDeleted,
      Table table) {
    Consumer<String> expiredFilesConsumer =
        getConsumerBasedOnDryRunConfig(dryRun, filesToBeDeleted, (HasTableOperations) table);
    ExpireSnapshots expireSnapshots =
        SparkActions.get().expireSnapshots(table).deleteWith(expiredFilesConsumer);
    entry.getValue().forEach(expireSnapshots::expireSnapshotId);
    return expireSnapshots.execute();
  }

  private static Consumer<String> getConsumerBasedOnDryRunConfig(
      boolean dryRun, List<String> filesToBeDeleted, HasTableOperations table) {
    Consumer<String> expiredFilesConsumer;
    if (!dryRun) {
      expiredFilesConsumer =
          fileName -> {
            filesToBeDeleted.add(fileName);
            // delete the expired files
            table.operations().io().deleteFile(fileName);
          };
    } else {
      expiredFilesConsumer = filesToBeDeleted::add;
    }
    return expiredFilesConsumer;
  }

  private static void updateMetadataLocation(BaseTable table) {
    try {
      BaseMetastoreTableOperations operations = (BaseMetastoreTableOperations) table.operations();
      FieldUtils.writeField(
          table.operations().current(),
          "metadataFileLocation",
          operations.currentMetadataLocation(),
          true);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static void dropExpireProcedureBranch(String expiryBranchName, NessieApiV1 api) {
    try {
      String lastCommitHash = getHeadCommitHash(api, expiryBranchName);
      api.deleteBranch().branchName(expiryBranchName).hash(lastCommitHash).delete();
    } catch (NessieNotFoundException | NessieConflictException e) {
      throw new RuntimeException(e);
    }
  }

  private static void createGCBranch(String expiryBranchName, NessieApiV1 api) {
    try {
      Reference branch = api.getReference().refName(expiryBranchName).get();
      if (branch != null) {
        throw new RuntimeException(
            String.format(
                "Branch should not be present with '%s' name "
                    + "as it will be dropped at the end of expiry operation",
                expiryBranchName));
      }
    } catch (NessieNotFoundException e) {
      GCUtil.createEmptyBranch(api, expiryBranchName);
    }
  }

  private static Map<String, List<Long>> getExpiredSnapshotsPerContentId(
      String runId, IdentifiedResultsRepo identifiedResultsRepo) {
    Dataset<Row> rowDataset = identifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
    // For the given run id, collect expired snapshots per content id.
    List<Row> rows =
        rowDataset
            .groupBy("contentId")
            .agg(functions.collect_set("snapshotId").as("snapshotIds"))
            .select("contentId", "snapshotIds")
            .collectAsList();

    Map<String, List<Long>> expiredSnapshotsPerContentId = new HashMap<>();
    rows.forEach(row -> expiredSnapshotsPerContentId.put(row.getString(0), row.getList(1)));
    return expiredSnapshotsPerContentId;
  }

  private static Map<String, List<String>> getMetadataLocationsPerContentId(
      String runId, IdentifiedResultsRepo identifiedResultsRepo) {
    Dataset<Row> rowDataset = identifiedResultsRepo.collectAllContentsAsDataSet(runId);
    // For the given run id, collect expired snapshots per content id.
    List<Row> rows =
        rowDataset
            .groupBy("contentId")
            .agg(functions.collect_set("metadataLocation").as("metadataLocations"))
            .select("contentId", "metadataLocations")
            .collectAsList();

    Map<String, List<String>> metadataLocationsPerContentId = new HashMap<>();
    rows.forEach(row -> metadataLocationsPerContentId.put(row.getString(0), row.getList(1)));
    return metadataLocationsPerContentId;
  }

  private static String updateRunId(
      String gcOutputTableName, String runId, IdentifiedResultsRepo identifiedResultsRepo) {
    if (runId == null) {
      Optional<String> latestCompletedRunID = identifiedResultsRepo.getLatestCompletedRunID();
      if (!latestCompletedRunID.isPresent()) {
        throw new RuntimeException(
            String.format(
                "No runId present in gc output table : %s, please execute %s first",
                gcOutputTableName, IdentifyExpiredSnapshotsProcedure.PROCEDURE_NAME));
      }
      runId = latestCompletedRunID.get();
    }
    return runId;
  }

  private static String getHeadCommitHash(NessieApiV1 api, String refName) {
    try {
      return api.getReference().refName(refName).get().getHash();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static Content getContentFromCommitHash(NessieApiV1 api, String hash, String contentId) {
    try {
      LogResponse.LogEntry logEntry =
          api.getCommitLog()
              .reference(Detached.of(hash))
              .maxRecords(1)
              .fetch(FetchOption.ALL)
              .get()
              .getLogEntries()
              .get(0);
      List<Content> contents =
          logEntry.getOperations().stream()
              .filter(operation -> operation instanceof Operation.Put)
              .map(operation -> ((Operation.Put) operation))
              .map(Operation.Put::getContent)
              .filter(content -> content.getId().equals(contentId))
              .collect(Collectors.toList());
      return contents.get(0);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
