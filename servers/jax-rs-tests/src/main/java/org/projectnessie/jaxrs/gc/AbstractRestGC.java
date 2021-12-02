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
package org.projectnessie.jaxrs.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.AssertionsForClassTypes;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.AbstractRest;
import org.projectnessie.jaxrs.AbstractTestRest;
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
import org.projectnessie.versioned.persist.gc.ContentValues;
import org.projectnessie.versioned.persist.gc.GCImpl;
import org.projectnessie.versioned.persist.gc.IdentifiedResult;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestGC extends AbstractRest {

  @NotNull
  List<LogEntry> fetchLogEntries(Branch branch, int limit) throws NessieNotFoundException {
    return getApi()
        .getCommitLog()
        .refName(branch.getName())
        .fetch(FetchOption.ALL)
        .maxRecords(limit)
        .get()
        .getLogEntries();
  }

  void fillExpectedContents(Branch branch, int limit, Map<String, Set<Content>> expected)
      throws NessieNotFoundException {
    List<LogEntry> logEntries = fetchLogEntries(branch, limit);
    logEntries.stream()
        .map(LogEntry::getOperations)
        .forEach(
            operations ->
                operations.stream()
                    .filter(op -> op instanceof Put)
                    .forEach(
                        op -> {
                          Content content = ((Put) op).getContent();
                          expected
                              .computeIfAbsent(content.getId(), k -> new HashSet<>())
                              .add(content);
                        }));
  }

  void performGc(
      Instant cutoffTimeStamp,
      Map<String, Instant> cutOffTimeStampPerRef,
      Map<String, Set<Content>> expectedLive,
      Map<String, Set<Content>> expectedExpired,
      Set<String> expectedContentIds) {
    SparkSession spark =
        SparkSession.builder().appName("test-nessie-gc").master("local[2]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    try {
      final Map<String, String> options = new HashMap<>();
      options.put(CONF_NESSIE_URI, getHttpClient().getBaseUri().toString());
      GCImpl gc = new GCImpl(options, cutoffTimeStamp, cutOffTimeStampPerRef);

      IdentifiedResult identifiedResult = gc.identifyExpiredContents(spark);
      // compare the expected contents against the actual gc output
      verify(identifiedResult, expectedLive, expectedExpired, expectedContentIds);
    } finally {
      spark.close();
    }
  }

  private void verify(
      IdentifiedResult contentValuesPerType,
      Map<String, Set<Content>> expectedLive,
      Map<String, Set<Content>> expectedExpired,
      Set<String> expectedContentIds) {
    AssertionsForClassTypes.assertThat(contentValuesPerType.getContentValues()).isNotNull();

    Set<Map.Entry<String, ContentValues>> entries =
        contentValuesPerType.getContentValues().entrySet();

    Map<String, Set<Content>> actualLive =
        entries.stream()
            .filter(e -> expectedContentIds.contains(e.getKey()))
            .filter(e -> !e.getValue().getLiveContents().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getLiveContents()));
    compareContents(actualLive, expectedLive);

    Map<String, Set<Content>> actualExpired =
        entries.stream()
            .filter(e -> expectedContentIds.contains(e.getKey()))
            .filter(e -> !e.getValue().getExpiredContents().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getExpiredContents()));
    compareContents(actualExpired, expectedExpired);
  }

  void compareContents(Map<String, Set<Content>> actual, Map<String, Set<Content>> expected) {
    AssertionsForClassTypes.assertThat(actual.size()).isEqualTo(expected.size());
    for (String key : expected.keySet()) {
      List<Content> expectedContents = new ArrayList<>(expected.get(key));
      expectedContents.sort(new ContentComparator());
      List<Content> actualContents = new ArrayList<>(actual.get(key));
      actualContents.sort(new ContentComparator());
      AssertionsForClassTypes.assertThat(expectedContents.size()).isEqualTo(actualContents.size());
      for (int i = 0; i < expectedContents.size(); i++) {
        compareContent(expectedContents.get(i), actualContents.get(i));
      }
    }
  }

  void compareContent(Content actual, Content expected) {
    if (expected.getType().equals(Content.Type.ICEBERG_TABLE)) {
      // exclude global state in comparison as it will change as per the commit and won't be same
      // as original.
      // compare only snapshot id for this content.
      assertThat((IcebergTable) actual)
          .extracting(IcebergTable::getSnapshotId)
          .isEqualTo(((IcebergTable) expected).getSnapshotId());
    } else if (expected.getType().equals(Content.Type.ICEBERG_VIEW)) {
      // exclude global state in comparison as it will change as per the commit and won't be same
      // as original.
      // compare only version id for this content.
      assertThat((IcebergView) actual)
          .extracting(IcebergView::getVersionId)
          .isEqualTo(((IcebergView) expected).getVersionId());
    } else {
      // contents that doesn't have global state can be compared fully.
      AssertionsForClassTypes.assertThat(actual).isEqualTo(expected);
    }
  }

  static class ContentComparator implements Comparator<Content> {
    @Override
    public int compare(Content c1, Content c2) {
      if (c1.getType().equals(Content.Type.ICEBERG_TABLE)) {
        return Long.compare(
            ((IcebergTable) c1).getSnapshotId(), ((IcebergTable) c2).getSnapshotId());
      } else {
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
