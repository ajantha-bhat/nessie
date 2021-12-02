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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;

public abstract class AbstractRestGCTest extends AbstractRestGC {

  @Test
  public void testSingleRefMultiTable() throws BaseNessieClientServerException {
    String prefix = "test1";
    Map<String, Set<Content>> expectedExpired = new HashMap<>();
    Map<String, Set<Content>> expectedLive = new HashMap<>();
    Branch branch = createBranch("singleRefMultiTable");
    // two commits for "table_1"
    CommitOutput table1 =
        commitSingleOp(
            prefix, branch, branch.getHash(), 42, "cid_1", "table_1", "file1", null, null);
    table1 =
        commitSingleOp(
            prefix, branch, table1.hash, 43, "cid_1", "table_1", "file2", table1.content, null);
    // two commits for "table_2"
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch, branch.getHash(), 42, "cid_2", "table_2", "file1", null, null);
    table2 =
        commitSingleOp(
            prefix, branch, table2.hash, 43, "cid_2", "table_2", "file2", table2.content, null);
    fillExpectedContents(branch, 4, expectedExpired);

    final Instant cutoffTime = Instant.now();

    // commits for "table_1"
    table1 =
        commitSingleOp(
            prefix, branch, table1.hash, 44, "cid_1", "table_1", "file3", table1.content, null);
    table1 =
        commitSingleOp(
            prefix, branch, table1.hash, 45, "cid_1", "table_1", "file4", table1.content, null);
    fillExpectedContents(branch, 2, expectedLive);
    // commits for "table_2"
    table2 =
        commitSingleOp(
            prefix, branch, table2.hash, 44, "cid_2", "table_2", "file3", table2.content, null);

    fillExpectedContents(branch, 1, expectedLive);

    // totally one live commit on table_2, two live commits on table_1.
    // two expired commits on each table_1 and table_2.
    performGc(
        cutoffTime,
        null,
        expectedLive,
        expectedExpired,
        new HashSet<>(Arrays.asList(prefix + "_cid_1", prefix + "_cid_2")));
  }

  @Test
  public void testSingleRefDropTable() throws BaseNessieClientServerException {
    String prefix = "test2";
    Map<String, Set<Content>> expectedExpired = new HashMap<>();
    Map<String, Set<Content>> expectedLive = new HashMap<>();
    Branch branch = createBranch("singleRefDropTable");
    // two commits for "table_1"
    CommitOutput table1 =
        commitSingleOp(
            prefix, branch, branch.getHash(), 42, "cid_1", "table_1", "file1", null, null);
    table1 =
        commitSingleOp(
            prefix, branch, table1.hash, 43, "cid_1", "table_1", "file2", table1.content, null);
    // two commits for "table_2"
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch, branch.getHash(), 42, "cid_2", "table_2", "file1", null, null);
    table2 =
        commitSingleOp(
            prefix, branch, table2.hash, 43, "cid_2", "table_2", "file2", table2.content, null);
    fillExpectedContents(branch, 4, expectedExpired);

    final Instant cutoffTime = Instant.now();

    // two commit for "table_1"
    table1 =
        commitSingleOp(
            prefix, branch, table1.hash, 44, "cid_1", "table_1", "file3", table1.content, null);
    table1 =
        commitSingleOp(
            prefix, branch, table1.hash, 45, "cid_1", "table_1", "file4", table1.content, null);
    fillExpectedContents(branch, 2, expectedLive);

    // commit for "table_2"
    table2 =
        commitSingleOp(
            prefix, branch, table2.hash, 44, "cid_2", "table_2", "file3", table2.content, null);
    // drop table "table_2"
    dropTableCommit(prefix, branch, table2.hash, "table_2");
    fillExpectedContents(branch, 2, expectedExpired);

    // two live commits on table_1.
    // two expired commits on each table_1 and three expired commits on table_2.
    performGc(
        cutoffTime,
        null,
        expectedLive,
        expectedExpired,
        new HashSet<>(Arrays.asList(prefix + "_cid_1", prefix + "_cid_2")));
  }

  @Test
  public void testSingleRefRenameTable() throws BaseNessieClientServerException {
    String prefix = "test3";
    Map<String, Set<Content>> expectedExpired = new HashMap<>();
    Map<String, Set<Content>> expectedLive = new HashMap<>();
    Branch branch = createBranch("singleRefRenameTable");
    // commits for "table_2"
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch, branch.getHash(), 42, "cid_2", "table_2", "file1", null, null);
    fillExpectedContents(branch, 1, expectedExpired);

    final Instant cutoffTime = Instant.now();

    table2 =
        commitSingleOp(
            prefix, branch, table2.hash, 43, "cid_2", "table_2", "file2", table2.content, null);
    fillExpectedContents(branch, 1, expectedLive);

    // commits for "table_2"
    table2 =
        commitSingleOp(
            prefix, branch, table2.hash, 44, "cid_2", "table_2", "file3", table2.content, null);

    // rename table "table_2" to "table_2_renamed"
    table2 =
        commitSingleOp(
            prefix,
            branch,
            table2.hash,
            44,
            "cid_2",
            "table_2_renamed",
            "file3",
            table2.content,
            "table_2");
    fillExpectedContents(branch, 1, expectedLive);

    // two live commits on table_2.
    // one expired commit on table_2.
    performGc(
        cutoffTime,
        null,
        expectedLive,
        expectedExpired,
        new HashSet<>(Arrays.asList(prefix + "_cid_1", prefix + "_cid_2")));
  }

  @Test
  public void testMultiRefDropRef() throws BaseNessieClientServerException {
    String prefix = "test4";
    Map<String, Set<Content>> expectedExpired = new HashMap<>();
    Map<String, Set<Content>> expectedLive = new HashMap<>();
    Branch branch1 = createBranch("multiRefDropRef_1");
    Branch branch2 = createBranch("multiRefDropRef_2");
    // commit for "table_1" on branch1
    CommitOutput table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, "cid_1", "table_1", "file1", null, null);
    fillExpectedContents(branch1, 1, expectedExpired);
    // commit for "table_2" on branch2
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch2, branch2.getHash(), 42, "cid_2", "table_2", "file1", null, null);
    fillExpectedContents(branch2, 1, expectedExpired);

    final Instant cutoffTime = Instant.now();

    // commits for "table_1" on branch 1
    table1 =
        commitSingleOp(
            prefix, branch1, table1.hash, 44, "cid_1", "table_1", "file3", table1.content, null);
    fillExpectedContents(branch1, 1, expectedLive);

    // commits for "table_2" on branch2
    table2 =
        commitSingleOp(
            prefix, branch2, table2.hash, 44, "cid_2", "table_2", "file3", table2.content, null);
    fillExpectedContents(branch2, 1, expectedExpired);

    // drop ref branch2
    deleteBranch(branch2.getName(), table2.hash);

    // one live commit on table_1.
    // one expired commits on each table_1 and two expired commits on table_2.
    performGc(
        cutoffTime,
        null,
        expectedLive,
        expectedExpired,
        new HashSet<>(Arrays.asList(prefix + "_cid_1", prefix + "_cid_2")));
  }

  @Test
  public void testMultiRefSharedTable() throws BaseNessieClientServerException {
    String prefix = "test5";
    Map<String, Set<Content>> expectedExpired = new HashMap<>();
    Map<String, Set<Content>> expectedLive = new HashMap<>();
    Branch branch1 = createBranch("multiRefSharedTable_1");
    // commit for "table_1" on branch1
    CommitOutput b1table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, "cid_1", "table_1", "file1", null, null);
    Branch branch2 =
        createBranch("multiRefSharedTable_2", Branch.of(branch1.getName(), b1table1.hash));
    Branch branch3 =
        createBranch("multiRefSharedTable_3", Branch.of(branch1.getName(), b1table1.hash));
    // commit for "table_1" on branch2
    CommitOutput b2table1 =
        commitSingleOp(
            prefix,
            branch2,
            branch2.getHash(),
            43,
            "cid_1",
            "table_1",
            "file2",
            b1table1.content,
            null);
    fillExpectedContents(branch1, 1, expectedExpired);
    fillExpectedContents(branch2, 1, expectedExpired);

    final Instant cutoffTime = Instant.now();

    // commit for "table_1" on branch1
    b1table1 =
        commitSingleOp(
            prefix,
            branch1,
            b1table1.hash,
            44,
            "cid_1",
            "table_1",
            "file3",
            b2table1.content,
            null);
    fillExpectedContents(branch1, 1, expectedExpired);

    // commit for "table_1" on branch2
    b2table1 =
        commitSingleOp(
            prefix,
            branch2,
            b2table1.hash,
            45,
            "cid_1",
            "table_1",
            "file4",
            b1table1.content,
            null);
    fillExpectedContents(branch2, 1, expectedLive);

    // commit for "table_1" on branch3
    CommitOutput b3table1 =
        commitSingleOp(
            prefix,
            branch3,
            branch3.getHash(),
            46,
            "cid_1",
            "table_1",
            "file5",
            b2table1.content,
            null);
    fillExpectedContents(branch3, 1, expectedExpired);
    // delete branch3
    deleteBranch(branch3.getName(), b3table1.hash);

    // drop table "table_1" on branch1
    dropTableCommit(prefix, branch1, b1table1.hash, "table_1");

    // one live commit and four expired commits on table_1.
    performGc(
        cutoffTime,
        null,
        expectedLive,
        expectedExpired,
        new HashSet<>(Collections.singletonList(prefix + "_cid_1")));
  }

  @Test
  public void testMultiRefCutoffTimeStampPerRef() throws BaseNessieClientServerException {
    String prefix = "test6";
    Map<String, Set<Content>> expectedExpired = new HashMap<>();
    Map<String, Set<Content>> expectedLive = new HashMap<>();
    Branch branch1 = createBranch("multiRefCutoffTimeStampPerRef_1");
    Branch branch2 = createBranch("multiRefCutoffTimeStampPerRef_2");
    // commit for "table_1" on branch1
    CommitOutput table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, "cid_1", "table_1", "file1", null, null);
    // commit for "table_2" on branch2
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch2, branch2.getHash(), 42, "cid_2", "table_2", "file1", null, null);
    fillExpectedContents(branch1, 1, expectedExpired);
    fillExpectedContents(branch2, 1, expectedExpired);

    Instant defaultCutoffTime = Instant.now();

    // commits for "table_1"
    table1 =
        commitSingleOp(
            prefix, branch1, table1.hash, 44, "cid_1", "table_1", "file3", table1.content, null);
    table1 =
        commitSingleOp(
            prefix, branch1, table1.hash, 45, "cid_1", "table_1", "file4", table1.content, null);
    fillExpectedContents(branch1, 2, expectedLive);

    // commits for "table_2"
    table2 =
        commitSingleOp(
            prefix, branch2, table2.hash, 44, "cid_2", "table_2", "file3", table2.content, null);
    fillExpectedContents(branch2, 1, expectedExpired);

    Instant branch2CutoffTime = Instant.now();
    Map<String, Instant> perRefCutoffTime = new HashMap<>();
    perRefCutoffTime.put(branch2.getName(), branch2CutoffTime);

    // commits for "table_2"
    table2 =
        commitSingleOp(
            prefix, branch2, table2.hash, 45, "cid_2", "table_2", "file4", table2.content, null);
    fillExpectedContents(branch2, 1, expectedLive);

    // table1 -> one expired and two live commits.
    // table 2 -> two expired and one live commit.
    performGc(
        defaultCutoffTime,
        perRefCutoffTime,
        expectedLive,
        expectedExpired,
        new HashSet<>(Arrays.asList(prefix + "_cid_1", prefix + "_cid_2")));
  }

  @Test
  public void testMultiRefAssignRef() throws BaseNessieClientServerException {
    String prefix = "test7";
    Map<String, Set<Content>> expectedExpired = new HashMap<>();
    Map<String, Set<Content>> expectedLive = new HashMap<>();
    Branch before = createBranch("multiRefAssignRef_0");
    Branch branch1 = createBranch("multiRefAssignRef_1");
    Branch branch2 = createBranch("multiRefAssignRef_2");
    // commit for "table_1" on branch1
    CommitOutput table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, "cid_1", "table_1", "file1", null, null);
    // commit for "table_2" on branch2
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch2, branch2.getHash(), 42, "cid_2", "table_2", "file1", null, null);
    fillExpectedContents(branch1, 1, expectedExpired);
    fillExpectedContents(branch2, 1, expectedExpired);

    final Instant cutoffTime = Instant.now();

    // commits for "table_1"
    table1 =
        commitSingleOp(
            prefix, branch1, table1.hash, 44, "cid_1", "table_1", "file3", table1.content, null);
    fillExpectedContents(branch1, 1, expectedLive);

    // commits for "table_2"
    table2 =
        commitSingleOp(
            prefix, branch2, table2.hash, 44, "cid_2", "table_2", "file3", table2.content, null);
    fillExpectedContents(branch2, 1, expectedExpired);

    // assign main to branch2
    getApi()
        .assignBranch()
        .branch(Branch.of(branch2.getName(), table2.hash))
        .assignTo(before)
        .assign();

    // one live commit on table_1.
    // one expired commits on each table_1 and two expired commits on table_2.
    performGc(
        cutoffTime,
        null,
        expectedLive,
        expectedExpired,
        new HashSet<>(Arrays.asList(prefix + "_cid_1", prefix + "_cid_2")));
  }
}
