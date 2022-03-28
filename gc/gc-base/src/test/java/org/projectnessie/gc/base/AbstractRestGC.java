/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.AbstractRest;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

public abstract class AbstractRestGC extends AbstractRest {

  @TempDir static File LOCAL_DIR;

  ObjectMapper objectMapper = new ObjectMapper();

  @NotNull
  List<LogEntry> fetchLogEntries(Branch branch, int numCommits) throws NessieNotFoundException {
    return getApi()
        .getCommitLog()
        .refName(branch.getName())
        .hashOnRef(branch.getHash())
        .fetch(FetchOption.ALL)
        .maxRecords(numCommits)
        .get()
        .getLogEntries();
  }

  void fillExpectedContents(Branch branch, int numCommits, IdentifiedResult expected)
      throws NessieNotFoundException {
    fetchLogEntries(branch, numCommits).stream()
        .map(LogEntry::getOperations)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .filter(op -> op instanceof Put)
        .forEach(
            op -> {
              Content content = ((Put) op).getContent();
              expected.addContent(
                  branch.getName(), content.getId(), GCUtil.serializeContent(content));
            });
  }

  void performGc(
      Instant cutoffTimeStamp,
      Map<String, Instant> cutOffTimeStampPerRef,
      IdentifiedResult expectedExpired,
      List<String> involvedRefs,
      boolean disableCommitProtection,
      Instant deadReferenceCutoffTime) {

    try (SparkSession sparkSession = getSparkSession()) {
      ImmutableGCParams.Builder builder = ImmutableGCParams.builder();
      final Map<String, String> options = new HashMap<>();
      options.put(CONF_NESSIE_URI, getUri().toString());
      if (disableCommitProtection) {
        // disable commit protection for test purposes.
        builder.commitProtectionDuration(Duration.ZERO);
      }
      ImmutableGCParams gcParams =
          builder
              .bloomFilterExpectedEntries(5L)
              .nessieClientConfigs(options)
              .deadReferenceCutOffTimeStamp(deadReferenceCutoffTime)
              .cutOffTimestampPerRef(cutOffTimeStampPerRef)
              .defaultCutOffTimestamp(cutoffTimeStamp)
              .nessieCatalogName("nessie")
              .outputTableRefName("gcRef")
              .outputTableIdentifier("db1.gc_results")
              .build();
      GCImpl gc = new GCImpl(gcParams);
      String runId = gc.identifyExpiredContents(sparkSession);

      IdentifiedResultsRepo identifiedResultsRepo =
          new IdentifiedResultsRepo(
              sparkSession,
              gcParams.getNessieCatalogName(),
              gcParams.getOutputTableRefName(),
              gcParams.getOutputTableIdentifier());
      IdentifiedResult identifiedResult = identifiedResultsRepo.collectExpiredContents(runId);
      // compare the expected contents against the actual gc output
      verify(identifiedResult, expectedExpired, involvedRefs);
    }
  }

  SparkSession getSparkSession() {
    SparkConf conf = new SparkConf();
    conf.set("spark.sql.catalog.nessie.uri", getUri().toString())
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.warehouse", LOCAL_DIR.toURI().toString())
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions");

    SparkSession spark =
        SparkSession.builder()
            .appName("test-nessie-gc")
            .master("local[2]")
            .config(conf)
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    return spark;
  }

  private void verify(
      IdentifiedResult identifiedResult,
      IdentifiedResult expectedExpired,
      List<String> involvedRefs) {
    assertThat(identifiedResult.getContentValues()).isNotNull();
    involvedRefs.forEach(
        ref ->
            compareContentsPerRef(
                identifiedResult.getContentValuesForReference(ref),
                expectedExpired.getContentValuesForReference(ref)));
  }

  private void compareContentsPerRef(
      Map<String, ContentValues> actualMap, Map<String, ContentValues> expectedMap) {
    assertThat(actualMap.size()).isEqualTo(expectedMap.size());
    actualMap
        .keySet()
        .forEach(
            contentId -> {
              compareContents(actualMap.get(contentId), expectedMap.get(contentId));
            });
  }

  private void compareContents(ContentValues actual, ContentValues expected) {
    if (actual == null && expected == null) {
      return;
    }
    assertThat(actual).isNotNull();
    assertThat(expected).isNotNull();
    assertThat(actual.getExpiredContents().size()).isEqualTo(expected.getExpiredContents().size());
    List<Content> expectedContents = getExpiredContents(expected);
    List<Content> actualContents = getExpiredContents(actual);
    for (int i = 0; i < expectedContents.size(); i++) {
      compareContent(expectedContents.get(i), actualContents.get(i));
    }
  }

  private List<Content> getExpiredContents(ContentValues expected) {
    return expected.getExpiredContents().stream()
        .map(GCUtil::deserializeContent)
        .sorted(new ContentComparator())
        .collect(Collectors.toList());
  }

  private void compareContent(Content actual, Content expected) {
    switch (expected.getType()) {
      case ICEBERG_TABLE:
        // exclude global state in comparison as it will change as per the commit and won't be same
        // as original.
        // compare only snapshot id for this content.
        assertThat((IcebergTable) actual)
            .extracting(IcebergTable::getSnapshotId)
            .isEqualTo(((IcebergTable) expected).getSnapshotId());
        break;
      case ICEBERG_VIEW:
        // exclude global state in comparison as it will change as per the commit and won't be same
        // as original.
        // compare only version id for this content.
        assertThat((IcebergView) actual)
            .extracting(IcebergView::getVersionId)
            .isEqualTo(((IcebergView) expected).getVersionId());
        break;
      default:
        // contents that doesn't have global state can be compared fully.
        assertThat(actual).isEqualTo(expected);
    }
  }

  static class ContentComparator implements Comparator<Content> {
    @Override
    public int compare(Content c1, Content c2) {
      switch (c1.getType()) {
        case ICEBERG_TABLE:
          return Long.compare(
              ((IcebergTable) c1).getSnapshotId(), ((IcebergTable) c2).getSnapshotId());
        case ICEBERG_VIEW:
          return Integer.compare(
              ((IcebergView) c1).getVersionId(), ((IcebergView) c2).getVersionId());
        default:
          return c1.getId().compareTo(c2.getId());
      }
    }
  }

  CommitOutput commitSingleOp(
      String prefix,
      Reference branch,
      String currentHash,
      long snapshotId,
      String contentId,
      String contentKey,
      String metadataFile,
      IcebergTable previous,
      String beforeRename)
      throws NessieNotFoundException, NessieConflictException {
    IcebergTable meta =
        IcebergTable.of(
            prefix + "_" + metadataFile, snapshotId, 42, 42, 42, prefix + "_" + contentId);
    CommitMultipleOperationsBuilder multiOp =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(currentHash)
            .commitMeta(
                CommitMeta.builder()
                    .author("someone")
                    .message("some commit")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Put.of(ContentKey.of(prefix + "_" + contentKey), meta, previous));

    if (beforeRename != null) {
      multiOp.operation(Operation.Delete.of(ContentKey.of(prefix + "_" + beforeRename)));
    }

    String nextHash = multiOp.commit().getHash();
    assertThat(currentHash).isNotEqualTo(nextHash);
    return new CommitOutput(nextHash, meta);
  }

  CommitOutput dropTableCommit(
      String prefix, Reference branch, String currentHash, String contentKey)
      throws NessieNotFoundException, NessieConflictException {
    String nextHash =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(currentHash)
            .commitMeta(
                CommitMeta.builder()
                    .author("someone")
                    .message("some commit")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Operation.Delete.of(ContentKey.of(prefix + "_" + contentKey)))
            .commit()
            .getHash();
    assertThat(currentHash).isNotEqualTo(nextHash);
    return new CommitOutput(nextHash, null);
  }

  static final class CommitOutput {
    final String hash;
    final IcebergTable content;

    CommitOutput(String hash, IcebergTable content) {
      this.hash = hash;
      this.content = content;
    }
  }
}
