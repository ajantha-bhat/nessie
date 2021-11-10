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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nessie GC procedure to expire unused snapshots, uses the information written by {@link
 * IdentifyLiveSnapshotsProcedure} via {@link IcebergGcRepo}.
 */
public class ExpireSnapshotsProcedure extends AbstractGcProcedure {

  public static final String PROCEDURE_NAME = "expire_snapshots";
  public static final int DEFAULT_IDENTIFY_RUNS = 3;
  public static final long DEFAULT_GRACE_TIME_MILLIS = TimeUnit.HOURS.toMillis(3);

  private static final Logger LOGGER = LoggerFactory.getLogger(ExpireSnapshotsProcedure.class);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("expire_branch", DataTypes.StringType),
        ProcedureParameter.optional("identify_runs", DataTypes.IntegerType),
        ProcedureParameter.optional("gc_table", DataTypes.StringType),
        ProcedureParameter.optional("catalog", DataTypes.StringType),
        ProcedureParameter.optional("gc_branch", DataTypes.StringType),
        ProcedureParameter.optional("do_expire", DataTypes.BooleanType),
        ProcedureParameter.optional("grace_time_millis", DataTypes.LongType)
      };

  public static final String OUTPUT_TIMESTAMP = "timestamp";
  public static final String OUTPUT_CONTENT_ID = "content_id";
  public static final String OUTPUT_TABLE = "table";
  public static final String OUTPUT_REF_NAME = "ref_name";
  public static final String OUTPUT_REF_HASH = "ref_hash";
  public static final String OUTPUT_VIA_HEAD = "via_head";
  public static final String OUTPUT_SUCCESS = "success";
  public static final String OUTPUT_SNAPSHOT_IDS = "snapshot_ids";
  public static final String OUTPUT_STATUS = "status";
  public static final String OUTPUT_FILES = "files";
  public static final String OUTPUT_EXPIRE_TABLE = "expire_table";
  public static final String OUTPUT_EXPIRED_DATA_FILES = "deleted_data_files";
  public static final String OUTPUT_EXPIRED_MANIFEST_LISTS = "deleted_manifest_lists";
  public static final String OUTPUT_EXPIRED_MANIFESTS = "deleted_manifests";

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(OUTPUT_TIMESTAMP, DataTypes.TimestampType, true, Metadata.empty()),
            new StructField(OUTPUT_CONTENT_ID, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OUTPUT_TABLE, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OUTPUT_REF_NAME, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OUTPUT_REF_HASH, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OUTPUT_VIA_HEAD, DataTypes.BooleanType, true, Metadata.empty()),
            new StructField(OUTPUT_SUCCESS, DataTypes.BooleanType, true, Metadata.empty()),
            new StructField(
                OUTPUT_SNAPSHOT_IDS,
                DataTypes.createArrayType(DataTypes.LongType),
                true,
                Metadata.empty()),
            new StructField(OUTPUT_STATUS, DataTypes.StringType, true, Metadata.empty()),
            new StructField(
                OUTPUT_FILES,
                DataTypes.createArrayType(DataTypes.StringType),
                true,
                Metadata.empty()),
            new StructField(OUTPUT_EXPIRE_TABLE, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OUTPUT_EXPIRED_DATA_FILES, DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_MANIFEST_LISTS, DataTypes.LongType, true, Metadata.empty()),
            new StructField(OUTPUT_EXPIRED_MANIFESTS, DataTypes.LongType, true, Metadata.empty())
          });
  static final String STATUS_SNAPSHOTS_EXPIRED = "Snapshots expired";
  static final String STATUS_SNAPSHOTS_EXPIRED_DRY = "Would expire snapshot";
  static final String STATUS_NO_EXPIRE = "No expire needed";

  private InternalRow resultRow(
      Timestamp timestamp,
      TableInformation tableInformation,
      Boolean viaHead,
      String status,
      String expireTable,
      Boolean success,
      List<Long> snapshotIds,
      ExpireSnapshots.Result expireSnapshotsResult,
      List<String> files) {
    return internalRow(
        DateTimeUtils.fromJavaTimestamp(timestamp),
        tableInformation.contentId,
        tableInformation.tableName,
        tableInformation.refName,
        tableInformation.refHash,
        viaHead,
        success,
        snapshotIds,
        status,
        files,
        expireTable,
        expireSnapshotsResult != null ? expireSnapshotsResult.deletedDataFilesCount() : null,
        expireSnapshotsResult != null ? expireSnapshotsResult.deletedManifestListsCount() : null,
        expireSnapshotsResult != null ? expireSnapshotsResult.deletedManifestsCount() : null);
  }

  public ExpireSnapshotsProcedure(TableCatalog currentCatalog) {
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
    return String.format(
        "Expires the Iceberg snapshots that are not in the set of live-snapshots collected by '%s' procedure.",
        IdentifyLiveSnapshotsProcedure.PROCEDURE_NAME);
  }

  @Override
  public InternalRow[] call(InternalRow internalRow) {
    String expireBranch = internalRow.getString(0);
    int identifyRuns = internalRow.isNullAt(1) ? DEFAULT_IDENTIFY_RUNS : internalRow.getInt(1);
    if (!internalRow.isNullAt(2)) {
      setGcTable(internalRow.getString(2));
    }
    if (!internalRow.isNullAt(3)) {
      setBranch(internalRow.getString(3));
    }
    if (!internalRow.isNullAt(4)) {
      setCatalog(internalRow.getString(4));
    }
    boolean doExpire = !internalRow.isNullAt(5) && internalRow.getBoolean(5);
    long graceTimeMillis =
        Math.max(0L, internalRow.isNullAt(6) ? DEFAULT_GRACE_TIME_MILLIS : internalRow.getLong(6));

    // TODO add a guardrail to check that there are at least 'identifyRuns' GC runs

    Map<String, ContentAndMetadataPointers> content = new HashMap<>();
    try (IcebergGcRepo repo = openRepo()) {
      repo.collectExpireableSnapshots(identifyRuns)
          .collectAsList()
          .forEach(
              row -> {
                String contentId = row.getString(0);
                List<String> liveMetadataPointers = row.getList(1);
                Map<String, String> referencesWithHashToKey = row.getJavaMap(2);
                content.put(
                    contentId,
                    new ContentAndMetadataPointers(
                        contentId, new HashSet<>(liveMetadataPointers), referencesWithHashToKey));
              });
    }

    try (NessieCatalog nessieCatalog = createNessieGcCatalog()) {
      // TODO this can probably be parallelized
      for (ContentAndMetadataPointers cmp : content.values()) {
        // TODO this can probably be parallelized
        cmp.liveSnapshotIds =
            cmp.liveMetadataPointers.stream()
                // TODO try-catch around loadTableMetadata + handling
                .map(nessieCatalog::loadTableMetadata)
                .map(TableMetadata::currentSnapshot)
                .filter(Objects::nonNull)
                .map(Snapshot::snapshotId)
                .collect(Collectors.toSet());
      }

      List<InternalRow> output = new ArrayList<>();

      for (ContentAndMetadataPointers cmp : content.values()) {
        for (Entry<String, String> entry : cmp.referencesWithHashToKey.entrySet()) {
          String refNameAndHash = entry.getKey();
          int idxHash = refNameAndHash.indexOf('#');
          String refName = refNameAndHash.substring(0, idxHash);
          String refHash = refNameAndHash.substring(idxHash + 1);
          String tableName = entry.getValue();
          TableInformation tableInformation =
              new TableInformation(cmp.contentId, tableName, refName, refHash);
          Timestamp timestamp = Timestamp.from(Instant.now());
          try {
            expireForTable(
                nessieCatalog,
                timestamp,
                tableInformation,
                cmp.liveSnapshotIds,
                expireBranch,
                output::add,
                doExpire,
                graceTimeMillis);
          } catch (NessieNotFoundException | NessieConflictException e) {
            LOGGER.error("Failed to expire snapshots for {}", tableInformation);
            output.add(
                resultRow(
                    timestamp,
                    tableInformation,
                    null,
                    "FAILURE: " + e,
                    null,
                    false,
                    null,
                    null,
                    null));
          }
        }
      }

      return output.toArray(new InternalRow[0]);
    } finally {
      // Rre-initialize the (Nessie) catalog, so things get reloaded to prevent any cache from
      // returning stale information with expired (removed/deleted) snapshots.
      CatalogPlugin catalogImpl = spark().sessionState().catalogManager().catalog(getCatalog());
      Map<String, String> catalogConf =
          AbstractGcProcedure.catalogConfWithRef(spark(), getCatalog(), null);
      catalogImpl.initialize(getCatalog(), new CaseInsensitiveStringMap(catalogConf));
    }
  }

  static final class TableInformation {
    final String contentId;
    final String tableName;
    final String refName;
    final String refHash;
    final ContentKey key;

    TableInformation(String contentId, String tableName, String refName, String refHash) {
      this.contentId = contentId;
      this.tableName = tableName;
      this.refName = refName;
      this.refHash = refHash;
      this.key = toKey(tableName);
    }

    String expireTableName() {
      return contentId.replaceAll("-", "_");
    }

    @Override
    public String toString() {
      return String.format(
          "content-id %s accessible as '%s' via %s at %s", contentId, tableName, refName, refHash);
    }
  }

  private void expireForTable(
      NessieCatalog nessieCatalog,
      Timestamp timestamp,
      TableInformation tableInformation,
      Set<Long> liveSnapshotIds,
      String expireBranch,
      Consumer<InternalRow> result,
      boolean doExpire,
      long graceTimeMillis)
      throws NessieNotFoundException, NessieConflictException {
    boolean commitToSourceBranch = canCommitToSourceBranch(nessieCatalog, tableInformation);

    String tableRef =
        commitToSourceBranch
            ? String.format("%s@%s", tableInformation.tableName, tableInformation.refName)
            : String.format(
                "%s@%s#%s",
                tableInformation.tableName, tableInformation.refName, tableInformation.refHash);
    Table table = nessieCatalog.loadTable(TableIdentifier.parse(tableRef));

    List<Long> expireSnapshotIds = new ArrayList<>();
    int snapshotCount = 0;
    for (Snapshot snap : table.snapshots()) {
      snapshotCount++;
      if (!liveSnapshotIds.contains(snap.snapshotId())
          && (System.currentTimeMillis() - snap.timestampMillis()) > graceTimeMillis) {
        // Expire a snapshot if it's not in the set of known live-snapshot-IDs and the snapshot
        // has not been created recently (i.e. within the last 'graceTimeMillis' milliseconds).
        expireSnapshotIds.add(snap.snapshotId());
      }
    }

    if (expireSnapshotIds.isEmpty()) {
      // No snapshots to expire

      LOGGER.info("Expiring no snapshots for Nessie {}", tableInformation);

      result.accept(
          resultRow(
              timestamp,
              tableInformation,
              commitToSourceBranch,
              STATUS_NO_EXPIRE,
              null,
              false,
              null,
              null,
              null));
      return;
    }

    LOGGER.info(
        "Expiring {} of {} snapshots for Nessie {} (expired snapshot IDs: {}) on reference {}",
        expireSnapshotIds.size(),
        snapshotCount,
        tableInformation,
        expireSnapshotIds,
        commitToSourceBranch ? tableInformation.refName : expireBranch);

    if (commitToSourceBranch) {
      // Perform Iceberg's expire-table action on the source branch.
      callIcebergExpireTable(
          timestamp, tableInformation, true, table, expireSnapshotIds, null, result, doExpire);
    } else {
      // Perform Iceberg's expire-table action on a special branch, because the table's not
      // modifiable (source is either a tag or not accessible via the branch's HEAD).
      expireUnmodifiableTable(
          nessieCatalog,
          timestamp,
          tableInformation,
          expireBranch,
          expireSnapshotIds,
          result,
          doExpire);
    }
  }

  private void callIcebergExpireTable(
      Timestamp timestamp,
      TableInformation tableInformation,
      boolean commitToSourceBranch,
      Table table,
      List<Long> expireSnapshotIds,
      String expireTable,
      Consumer<InternalRow> result,
      boolean doExpire) {

    ExpireSnapshots.Result expireResult = null;
    List<String> files = new ArrayList<>();
    if (doExpire) {
      ExpireSnapshots expire =
          SparkActions.get(spark()).expireSnapshots(table).deleteWith(files::add);
      expireSnapshotIds.forEach(expire::expireSnapshotId);
      expireResult = expire.execute();
    }

    result.accept(
        resultRow(
            timestamp,
            tableInformation,
            commitToSourceBranch,
            doExpire ? STATUS_SNAPSHOTS_EXPIRED : STATUS_SNAPSHOTS_EXPIRED_DRY,
            expireTable,
            true,
            expireSnapshotIds,
            expireResult,
            files));
  }

  private void expireUnmodifiableTable(
      NessieCatalog nessieCatalog,
      Timestamp timestamp,
      TableInformation tableInformation,
      String expireBranch,
      List<Long> expireSnapshotIds,
      Consumer<InternalRow> result,
      boolean doExpire)
      throws NessieNotFoundException, NessieConflictException {
    Table table;
    ensureBranchExists(nessieCatalog, expireBranch, true, "Iceberg-expire-snapshots actions");

    Optional<IcebergTable> icebergTable =
        Optional.ofNullable(
                nessieCatalog
                    .getApi()
                    .getContent()
                    .key(tableInformation.key)
                    .refName(tableInformation.refName)
                    .hashOnRef(tableInformation.refHash)
                    .get()
                    .get(tableInformation.key))
            .flatMap(c -> c.unwrap(IcebergTable.class));
    if (icebergTable.isPresent()) {

      String expireTableName = tableInformation.expireTableName();
      nessieCatalog
          .getApi()
          .commitMultipleOperations()
          .commitMeta(
              CommitMeta.fromMessage(
                  String.format("Create GC mirror table for %s", tableInformation)))
          .operation(
              Operation.Put.of(
                  ContentKey.of(expireTableName), icebergTable.get(), icebergTable.get()))
          .branchName(expireBranch)
          .commit();

      table =
          nessieCatalog.loadTable(
              TableIdentifier.parse(String.format("%s@%s", expireTableName, expireBranch)));
      callIcebergExpireTable(
          timestamp,
          tableInformation,
          false,
          table,
          expireSnapshotIds,
          expireTableName,
          result,
          doExpire);
    } else {
      String failureMessage = String.format("Could not find IcebergTable %s", tableInformation);

      result.accept(
          resultRow(
              timestamp, tableInformation, false, failureMessage, null, false, null, null, null));
    }
  }

  private boolean canCommitToSourceBranch(
      NessieCatalog nessieCatalog, TableInformation tableInformation)
      throws NessieNotFoundException {
    Reference ref = nessieCatalog.getApi().getReference().refName(tableInformation.refName).get();
    if (ref instanceof Branch) {
      Optional<IcebergTable> icebergTable =
          Optional.ofNullable(
                  nessieCatalog
                      .getApi()
                      .getContent()
                      .key(tableInformation.key)
                      .refName(tableInformation.refName)
                      .get()
                      .get(tableInformation.key))
              .flatMap(c -> c.unwrap(IcebergTable.class));
      // If the table is reachable via the HEAD of the branch, perform expireTable on the HEAD of
      // that branch (return true), otherwise there's a conflicting commit (return false).
      return icebergTable.isPresent()
          && icebergTable.get().getId().equals(tableInformation.contentId);
    } else {
      // Tag

      // Tags won't get modified - expire, but don't commit
      return false;
    }
  }

  static ContentKey toKey(String table) {
    TableIdentifier tableIdentifier = TableIdentifier.parse(table);
    List<String> identifiers = new ArrayList<>();
    if (tableIdentifier.hasNamespace()) {
      identifiers.addAll(Arrays.asList(tableIdentifier.namespace().levels()));
    }
    identifiers.add(tableIdentifier.name());

    return ContentKey.of(identifiers);
  }
}
