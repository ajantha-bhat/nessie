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
package org.projectnessie.api.v2;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.projectnessie.api.v2.params.CommitLogParams;
import org.projectnessie.api.v2.params.DiffParams;
import org.projectnessie.api.v2.params.EntriesParams;
import org.projectnessie.api.v2.params.GetReferenceParams;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.api.v2.params.ReferencesParams;
import org.projectnessie.api.v2.params.Transplant;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.SingleReferenceResponse;
import org.projectnessie.model.Validation;

/**
 * Interface for working with "trees", that is with collections of contents at a particular point in
 * the change history, organized in trees by their respective namespaces.
 *
 * <p>A tree is identified by a reference (branch, tag or a "detached" commit hash).
 *
 * <p>Only branches and tags can be created / deleted / assigned via respective "*Reference"
 * methods. Commits cannot be deleted and get created via commit / merge / transplant operations.
 */
public interface TreeApi {

  // Note: When substantial changes in Nessie API (this and related interfaces) are made
  // the API version number reported by NessieConfiguration.getMaxSupportedApiVersion()
  // should be increased as well.

  /**
   * Get all references.
   *
   * @return A {@link ReferencesResponse} instance containing all references.
   */
  ReferencesResponse getAllReferences(ReferencesParams params);

  /**
   * Create a new reference.
   *
   * <p>The type of {@code reference}, which can be either a {@link Branch} or {@link
   * org.projectnessie.model.Tag}, determines the type of the reference to be created.
   *
   * <p>{@link Reference#getName()} defines the name of the reference to be created, {@link
   * Reference#getHash()} is the hash of the created reference, the HEAD of the created reference.
   * {@code sourceRefName} is the name of the reference which contains {@link Reference#getHash()},
   * and must be present if {@link Reference#getHash()} is present.
   *
   * <p>Specifying no {@link Reference#getHash()} means that the new reference will be created "at
   * the beginning of time".
   */
  SingleReferenceResponse createReference(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String name,
      @Valid @NotNull Reference.ReferenceType type,
      @Valid @Nullable Reference sourceRef)
      throws NessieNotFoundException, NessieConflictException;

  /** Get details of a particular ref, if it exists. */
  SingleReferenceResponse getReferenceByName(@Valid @NotNull GetReferenceParams params)
      throws NessieNotFoundException;

  /**
   * Retrieve objects for a ref, potentially truncated by the backend.
   *
   * <p>Retrieves up to {@code maxRecords} objects for the given named reference (tag or branch).
   * The backend <em>may</em> respect the given {@code max} records hint, but return less or more
   * entries. Backends may also cap the returned entries at a hard-coded limit, the default REST
   * server implementation has such a hard-coded limit.
   *
   * <p>Invoking {@code getEntries()} does <em>not</em> guarantee to return all commit log entries
   * of a given reference, because the result can be truncated by the backend.
   *
   * <p>To implement paging, check {@link EntriesResponse#isHasMore() EntriesResponse.isHasMore()}
   * and, if {@code true}, pass the value of {@link EntriesResponse#getToken()
   * EntriesResponse.getToken()} in the next invocation of {@code getEntries()} as the {@code
   * pageToken} parameter.
   *
   * <p>See {@code org.projectnessie.client.StreamingUtil} in {@code nessie-client}.
   */
  EntriesResponse getEntries(
      @Valid
          @NotNull
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          String refName,
      @Valid @NotNull EntriesParams params)
      throws NessieNotFoundException;

  /**
   * Retrieve the commit log for a ref, potentially truncated by the backend.
   *
   * <p>Retrieves up to {@code maxRecords} commit-log-entries starting at the HEAD of the given
   * named reference (tag or branch). The backend <em>may</em> respect the given {@code max} records
   * hint, but return less or more entries. Backends may also cap the returned entries at a
   * hard-coded limit, the default REST server implementation has such a hard-coded limit.
   *
   * <p>Invoking {@code getCommitLog()} does <em>not</em> guarantee to return all commit log entries
   * of a given reference, because the result can be truncated by the backend.
   *
   * <p>To implement paging, check {@link LogResponse#isHasMore() LogResponse.isHasMore()} and, if
   * {@code true}, pass the value of {@link LogResponse#getToken() LogResponse.getToken()} in the
   * next invocation of {@code getCommitLog()} as the {@code pageToken} parameter.
   *
   * <p>See {@code org.projectnessie.client.StreamingUtil} in {@code nessie-client}.
   */
  LogResponse getCommitLog(
      @Valid
          @NotNull
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          String ref,
      @Valid @NotNull CommitLogParams params)
      throws NessieNotFoundException;

  /**
   * Returns a set of content differences between two given references.
   *
   * @param params The {@link DiffParams} that includes the parameters for this API call.
   * @return A set of diff values that show the difference between two given references.
   */
  DiffResponse getDiff(@Valid @NotNull DiffParams params) throws NessieNotFoundException;

  /**
   * Update a reference's HEAD to point to a different commit.
   *
   * @param type Optional expected type of reference being assigned. Will be validated if present.
   */
  SingleReferenceResponse assignReference(
      @Valid Reference.ReferenceType type,
      @Valid
          @NotNull
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          String reference,
      @Valid @NotNull Reference assignTo)
      throws NessieNotFoundException, NessieConflictException;

  /**
   * Delete a named reference.
   *
   * @param type Optional expected type of reference being deleted. Will be validated if present.
   */
  SingleReferenceResponse deleteReference(
      @Valid Reference.ReferenceType type,
      @Valid
          @NotNull
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          String reference)
      throws NessieConflictException, NessieNotFoundException;

  /** Cherry-pick a set of commits into a branch. */
  MergeResponse transplantCommitsIntoBranch(
      @Valid
          @NotNull
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          String branch,
      @Valid Transplant transplant)
      throws NessieNotFoundException, NessieConflictException;

  /** Merge commits from any reference onto a branch. */
  MergeResponse mergeRefIntoBranch(
      @Valid
          @NotNull
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          String branch,
      @Valid @NotNull Merge merge)
      throws NessieNotFoundException, NessieConflictException;

  /**
   * Commit multiple operations against the given branch expecting that branch to have the given
   * hash as its latest commit. The hash in the successful response contains the hash of the commit
   * that contains the operations of the invocation.
   *
   * @param branch Branch to change, defaults to default branch.
   * @param operations {@link Operations} to apply
   * @return updated {@link Branch} objects with the hash of the new HEAD
   * @throws NessieNotFoundException if {@code branchName} could not be found
   * @throws NessieConflictException if the operations could not be applied to some conflict, which
   *     is either caused by a conflicting commit or concurrent commits.
   */
  CommitResponse commitMultipleOperations(
      @Valid
          @NotNull
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          String branch,
      @Valid @NotNull Operations operations)
      throws NessieNotFoundException, NessieConflictException;

  /**
   * This operation returns the {@link Content} for a {@link ContentKey} in a named-reference (a
   * {@link org.projectnessie.model.Branch} or {@link org.projectnessie.model.Tag}).
   *
   * <p>If the table-metadata is tracked globally (Iceberg), Nessie returns a {@link Content}
   * object, that contains the most up-to-date part for the globally tracked part (Iceberg:
   * table-metadata) plus the per-Nessie-reference/hash specific part (Iceberg: snapshot-ID,
   * schema-ID, partition-spec-ID, default-sort-order-ID).
   *
   * @param key the {@link ContentKey}s to retrieve
   * @param ref named-reference to retrieve the content for
   * @return list of {@link GetMultipleContentsResponse.ContentWithKey}s
   * @throws NessieNotFoundException if {@code ref} or {@code hashOnRef} does not exist
   */
  ContentResponse getContent(
      @Valid ContentKey key,
      @Valid
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          String ref)
      throws NessieNotFoundException;

  /**
   * Similar to {@link #getContent(ContentKey, String)}, but takes multiple {@link ContentKey}s and
   * returns the {@link Content} for the one or more {@link ContentKey}s in a named-reference (a
   * {@link org.projectnessie.model.Branch} or {@link org.projectnessie.model.Tag}).
   *
   * <p>If the table-metadata is tracked globally (Iceberg), Nessie returns a {@link Content}
   * object, that contains the most up-to-date part for the globally tracked part (Iceberg:
   * table-metadata) plus the per-Nessie-reference/hash specific part (Iceberg: snapshot-id,
   * schema-id, partition-spec-id, default-sort-order-id).
   *
   * @param ref named-reference to retrieve the content for
   * @param request the {@link ContentKey}s to retrieve
   * @return list of {@link GetMultipleContentsResponse.ContentWithKey}s
   * @throws NessieNotFoundException if {@code ref} or {@code hashOnRef} does not exist
   */
  GetMultipleContentsResponse getMultipleContents(
      @Valid
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          String ref,
      @Valid @NotNull GetMultipleContentsRequest request)
      throws NessieNotFoundException;
}
