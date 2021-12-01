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
package org.projectnessie.versioned.persist.gc;

import static java.util.Arrays.asList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.api.http.HttpContentApi;
import org.projectnessie.api.http.HttpTreeApi;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.client.http.v1api.HttpApiV1;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Reference;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.rest.RestContentResource;
import org.projectnessie.services.rest.RestTreeResource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.store.PersistVersionStore;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;

@ExtendWith(DatabaseAdapterExtension.class)
abstract class AbstractGCTest {
  @NessieDbAdapter protected static DatabaseAdapter databaseAdapter;

  @ParameterizedTest
  @MethodSource("datasetsSource")
  void datasets(Dataset dataset) {
    dataset.applyToAdapter(databaseAdapter);

    datasetsTest(dataset, databaseAdapter);
  }

  static List<Dataset> datasetsSource() {
    return produceDatasetsSource(Dataset::new);
  }

  static final TableCommitMetaStoreWorker STORE_WORKER = new TableCommitMetaStoreWorker();

  static final String DEFAULT_BRANCH = "main";

  // Note: since the GC implementation works with absolute timestamps, we can use absolute
  // timestamps in our test data sets as well.

  static final Instant NOW = Instant.ofEpochSecond(1000);
  static final Instant NOT_LIVE = Instant.ofEpochSecond(500);

  static ContentKey TABLE_ONE = ContentKey.of("table", "one");
  static ContentKey TABLE_TWO = ContentKey.of("table", "two");
  static ContentKey TABLE_THREE = ContentKey.of("table", "three");
  static ContentKey TABLE_FOUR = ContentKey.of("table", "four");
  static String CID_ONE = "table-one";
  static String CID_TWO = "table-two";
  static String CID_THREE = "table-three";
  static String CID_FOUR = "table-four";

  /**
   * Common test implementation to verify the expected GC results for a specific data set. This test
   * is exactly the same for mocked and "real" {@link DatabaseAdapter}s, datasets are exactly the
   * same, timestamps are exactly the same, hashes however do vary.
   */
  protected void datasetsTest(Dataset dataset, DatabaseAdapter databaseAdapter) {
    PersistVersionStore<Content, CommitMeta, Content.Type> versionStore =
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
    HttpTreeApi treeApi = new RestTreeResource(serverConfig, versionStore, null);
    HttpContentApi contentApi = new RestContentResource(serverConfig, versionStore, null);
    HttpApiV1 api = new HttpApiV1(new NessieApiClient(null, treeApi, contentApi, null));

    GC.Builder gcBuilder = GC.builder().withApi(api).withDefaultCutOffTimeStamp(NOT_LIVE);
    dataset.cutOffTimeStampComputations.forEach(
        gcBuilder::addCutOffTimeStampPerReferenceComputation);
    GC gc = gcBuilder.build();

    ContentValuesCollector<IcebergExpiredContentValues> contentValuesCollector =
        new ContentValuesCollector<>(IcebergExpiredContentValues::new);

    GCResult<IcebergExpiredContentValues> contentValuesPerType =
        gc.performGC(contentValuesCollector);

    dataset.verify(contentValuesPerType);
  }

  /** Produces the datasets verified via {@link #datasetsTest(Dataset, DatabaseAdapter)}. */
  static List<Dataset> produceDatasetsSource(Function<String, Dataset> datasetFactory) {
    return asList(
        datasetSingleTableSingleRefCutOffTime(
            datasetFactory.apply("datasetSingleTableSingleRefCutOffTime")),
        datasetSingleTableMultiRefCutOffTime(
            datasetFactory.apply("datasetSingleTableMultiRefCutOffTime")),
        datasetDropTableSingleRef(datasetFactory.apply("datasetDropTableSingleRef")),
        datasetMultiTableMultiRef(datasetFactory.apply("datasetMultiTableMultiRef")),
        datasetMultiTableMultiRefWithDifferentCutoffTime(
            datasetFactory.apply("datasetMultiTableMultiRefWithDifferentCutoffTime")),
        datasetRename(datasetFactory.apply("datasetRename")));
  }

  static Dataset datasetSingleTableSingleRefCutOffTime(Dataset ds) {
    return ds
        //
        .commit(NOT_LIVE.minusSeconds(10))
        .put(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), true)
        .build()
        //
        .commit(NOT_LIVE.minusSeconds(8))
        .put(TABLE_ONE, IcebergTable.of("meta2", 43, 42, 42, 42, CID_ONE), true)
        .build()
        //
        .commit(NOT_LIVE)
        .put(TABLE_ONE, IcebergTable.of("meta3", 44, 42, 42, 42, CID_ONE), false)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(2))
        .put(TABLE_ONE, IcebergTable.of("meta4", 45, 42, 42, 42, CID_ONE), false)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(4))
        .put(TABLE_ONE, IcebergTable.of("meta5", 46, 42, 42, 42, CID_ONE), false)
        .build();
  }

  static Dataset datasetSingleTableMultiRefCutOffTime(Dataset ds) {
    return ds
        //
        .commit(NOT_LIVE.minusSeconds(10))
        .put(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), true)
        .build()
        //
        .branch("branch-1")
        .commit(NOT_LIVE.minusSeconds(8))
        .put(TABLE_ONE, IcebergTable.of("meta2", 43, 42, 42, 42, CID_ONE), false)
        .build()
        //
        .branch(DEFAULT_BRANCH)
        .commit(NOT_LIVE.minusSeconds(6))
        .put(TABLE_ONE, IcebergTable.of("meta3", 44, 42, 42, 42, CID_ONE), true)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(2))
        .put(TABLE_ONE, IcebergTable.of("meta4", 45, 42, 42, 42, CID_ONE), false)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(4))
        .put(TABLE_ONE, IcebergTable.of("meta5", 46, 42, 42, 42, CID_ONE), false)
        .build();
  }

  static Dataset datasetDropTableSingleRef(Dataset ds) {
    return ds
        //
        .commit(NOT_LIVE.plusSeconds(10))
        .put(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), true)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(12))
        .put(TABLE_ONE, IcebergTable.of("meta2", 43, 42, 42, 42, CID_ONE), true)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(14))
        .put(TABLE_ONE, IcebergTable.of("meta3", 46, 42, 42, 42, CID_ONE), true)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(16))
        .delete(TABLE_ONE)
        .build();
  }

  // todo: add expire per ref testcase, add drop table testcase and clean up existing testcase flex

  static Dataset datasetMultiTableMultiRef(Dataset ds) {
    return ds
        //
        .commit(NOT_LIVE.minusSeconds(2))
        .put(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), false)
        .put(TABLE_TWO, IcebergTable.of("meta2", 42, 42, 42, 42, CID_TWO), true)
        .build()
        //
        .commit(NOT_LIVE.minusSeconds(1))
        .delete(TABLE_TWO)
        .build()
        //
        .commit(NOT_LIVE)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(1))
        .put(TABLE_THREE, IcebergTable.of("meta3", 42, 42, 42, 42, CID_THREE), false)
        .build()
        //
        // create branches
        .branch("branch-1")
        .branch("branch-2")
        //
        .branch("branch-1")
        .commit(NOW.minusSeconds(10))
        .delete(TABLE_ONE)
        .build()
        //
        .branch("branch-2")
        .commit(NOW.minusSeconds(10))
        .delete(TABLE_THREE)
        .build();
  }

  static Dataset datasetMultiTableMultiRefWithDifferentCutoffTime(Dataset ds) {
    return ds
        //
        .cutOffTimeStampComputation(
            ref -> {
              if (ref.getName().equals("branch-1")) {
                return NOT_LIVE.plusSeconds(5);
              } else if ((ref.getName().equals("branch-2"))) {
                return NOT_LIVE.plusSeconds(10);
              } else {
                return null;
              }
            })
        //
        .commit(NOT_LIVE.minusSeconds(2))
        .put(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), false)
        .put(TABLE_TWO, IcebergTable.of("meta2", 42, 42, 42, 42, CID_TWO), true)
        .build()
        //
        .commit(NOT_LIVE.minusSeconds(1))
        .delete(TABLE_TWO)
        .build()
        //
        .commit(NOT_LIVE)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(1))
        .put(TABLE_THREE, IcebergTable.of("meta3", 42, 42, 42, 42, CID_THREE), false)
        .build()
        //
        // create branches
        .branch("branch-1")
        .branch("branch-2")
        //
        .branch("branch-1")
        .commit(NOT_LIVE.plusSeconds(3))
        .put(TABLE_THREE, IcebergTable.of("meta4", 43, 42, 42, 42, CID_THREE), true)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(4))
        .put(TABLE_THREE, IcebergTable.of("meta5", 44, 42, 42, 42, CID_THREE), true)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(6))
        .put(TABLE_THREE, IcebergTable.of("meta5", 45, 42, 42, 42, CID_THREE), false)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(7))
        .put(TABLE_THREE, IcebergTable.of("meta5", 46, 42, 42, 42, CID_THREE), false)
        .build()
        //
        .branch("branch-2")
        .commit(NOT_LIVE.plusSeconds(3))
        .put(TABLE_TWO, IcebergTable.of("meta4", 43, 42, 42, 42, CID_TWO), true)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(4))
        .put(TABLE_TWO, IcebergTable.of("meta5", 44, 42, 42, 42, CID_TWO), true)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(16))
        .put(TABLE_TWO, IcebergTable.of("meta5", 45, 42, 42, 42, CID_TWO), false)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(17))
        .put(TABLE_TWO, IcebergTable.of("meta5", 46, 42, 42, 42, CID_TWO), false)
        .build()
        //
        .commit(NOW.minusSeconds(10))
        .delete(TABLE_THREE)
        .build();
  }

  static Dataset datasetRename(Dataset ds) {
    return ds
        //
        .commit(NOT_LIVE.minusSeconds(2))
        .put(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), false)
        .put(TABLE_TWO, IcebergTable.of("meta2", 42, 42, 42, 42, CID_TWO), true)
        .build()
        //
        .commit(NOT_LIVE.minusSeconds(1))
        .delete(TABLE_TWO)
        .build()
        //
        .commit(NOT_LIVE)
        .build()
        //
        .commit(NOT_LIVE.plusSeconds(1))
        .put(TABLE_THREE, IcebergTable.of("meta3", 42, 42, 42, 42, CID_THREE), false)
        .build()
        //
        // create branches
        .branch("branch-1")
        .branch("branch-2")
        //
        .branch("branch-1")
        .commit(NOW.minusSeconds(10))
        .delete(TABLE_ONE)
        .build()
        //
        .branch("branch-2")
        .commit(NOW.minusSeconds(11))
        .delete(TABLE_ONE)
        .put(TABLE_FOUR, IcebergTable.of("meta2_1", 43, 42, 42, 42, CID_TWO), false)
        .build()
        .commit(NOW.minusSeconds(10))
        .delete(TABLE_THREE)
        .build();
  }

  static final class CommitBuilder {
    final Instant ts;
    final NamedRef ref;
    final Dataset dataset;
    final Map<ContentId, ByteString> globals = new HashMap<>();
    final List<KeyWithBytes> puts = new ArrayList<>();
    final List<Key> deletes = new ArrayList<>();

    CommitBuilder(Dataset dataset, NamedRef ref, Instant ts) {
      this.ts = ts;
      this.ref = ref;
      this.dataset = dataset;
    }

    /**
     * Adds a "Put operation" for a key/content-id/content-value and whether that value is expected
     * to be live or non-live after GC.
     */
    CommitBuilder put(ContentKey key, Content content, boolean expectExpired) {
      ContentId cid = ContentId.of(content.getId());
      puts.add(
          KeyWithBytes.of(
              Key.of(key.getElements().toArray(new String[0])),
              cid,
              (byte) STORE_WORKER.getType(content).ordinal(),
              STORE_WORKER.toStoreOnReferenceState(content)));
      if (STORE_WORKER.requiresGlobalState(content)) {
        globals.put(cid, STORE_WORKER.toStoreGlobalState(content));
      }
      Map<String, Set<Object>> expect = expectExpired ? dataset.expectExpired : dataset.expectLive;
      Object o = content;
      if (content instanceof IcebergTable) {
        // just add snapshot id instead of whole content
        o = ((IcebergTable) content).getSnapshotId();
      }
      expect.computeIfAbsent(content.getId(), x -> new HashSet<>()).add(o);
      return this;
    }

    /** Adds a "Delete operation" for a key. */
    CommitBuilder delete(ContentKey delete) {
      deletes.add(Key.of(delete.getElements().toArray(new String[0])));
      return this;
    }

    /** Build and record this commit. */
    Dataset build() {
      return dataset.recordCommit(
          adapter -> {
            ImmutableCommitAttempt.Builder commit =
                ImmutableCommitAttempt.builder()
                    .commitMetaSerialized(
                        STORE_WORKER
                            .getMetadataSerializer()
                            .toBytes(
                                ImmutableCommitMeta.builder()
                                    .commitTime(ts)
                                    .committer("")
                                    .authorTime(ts)
                                    .author("")
                                    .message("foo message")
                                    .build()))
                    .putAllGlobal(globals)
                    .commitToBranch((BranchName) ref)
                    .addAllPuts(puts)
                    .addAllDeletes(deletes);
            try {
              adapter.commit(commit.build());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  static class Dataset {
    NamedRef currentRef;
    final List<Consumer<DatabaseAdapter>> ops = new ArrayList<>();
    final Set<NamedRef> knownRefs = new HashSet<>();
    final String name;
    final Map<String, Set<Object>> expectLive = new HashMap<>();
    final Map<String, Set<Object>> expectExpired = new HashMap<>();
    final List<Function<Reference, Instant>> cutOffTimeStampComputations = new ArrayList<>();

    Dataset(String name) {
      this.name = name;
      currentRef = BranchName.of(DEFAULT_BRANCH);
    }

    Dataset cutOffTimeStampComputation(Function<Reference, Instant> liveAfterComputation) {
      this.cutOffTimeStampComputations.add(liveAfterComputation);
      return this;
    }

    /**
     * Switches to the given branch, creates the branch if necessary from the current branch's HEAD.
     */
    Dataset branch(String branchName) {
      NamedRef ref = BranchName.of(branchName);
      NamedRef current = this.currentRef;
      this.currentRef = ref;
      if (knownRefs.add(ref)) {
        ops.add(
            adapter -> {
              try {
                adapter.create(
                    ref, adapter.namedRef(current.getName(), GetNamedRefsParams.DEFAULT).getHash());
              } catch (ReferenceAlreadyExistsException e) {
                // Do nothing, just switch the branch
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
      }
      return this;
    }

    /**
     * Starts building a commit with the given timestamp as the system-commit-timestamp recorded in
     * the commit.
     */
    CommitBuilder commit(Instant timestamp) {
      return new CommitBuilder(this, currentRef, timestamp);
    }

    /**
     * Verifies that the {@link GCResult} contains the expected content-ids and live/non-live
     * content-values.
     */
    void verify(GCResult<IcebergExpiredContentValues> contentValuesPerType) {
      assertThat(contentValuesPerType.getContentValues()).isNotNull();

      Map<String, IcebergExpiredContentValues> got = contentValuesPerType.getContentValues();
      assertThat(
              got.entrySet().stream()
                  .filter(e -> !e.getValue().getLiveSnapshotIds().isEmpty())
                  .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().getLiveSnapshotIds())))
          .describedAs("expected live content in GCResult'")
          .isEqualTo(expectLive);

      assertThat(
              got.entrySet().stream()
                  .filter(e -> !e.getValue().getExpiredSnapshotIds().isEmpty())
                  .collect(
                      Collectors.toMap(Entry::getKey, e -> e.getValue().getExpiredSnapshotIds())))
          .describedAs("expected expired content in GCResult'")
          .isEqualTo(expectExpired);
    }

    Dataset recordCommit(Consumer<DatabaseAdapter> commitProducer) {
      ops.add(commitProducer);
      return this;
    }

    void applyToAdapter(DatabaseAdapter databaseAdapter) {
      for (Consumer<DatabaseAdapter> op : ops) {
        op.accept(databaseAdapter);
      }
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
