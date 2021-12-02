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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

/**
 * Encapsulates the logic to retrieve all content including their content keys over all commits in
 * all named-references.
 */
public class GCImpl {
  private final Map<String, String> configuration;
  private final Instant defaultCutOffTimeStamp;
  private final Map<String, Instant> cutOffTimestampPerRef;

  public GCImpl(
      Map<String, String> configuration,
      Instant defaultCutOffTimeStamp,
      Map<String, Instant> cutOffTimestampPerRef) {
    Objects.requireNonNull(configuration);
    Objects.requireNonNull(defaultCutOffTimeStamp);
    this.configuration = configuration;
    this.defaultCutOffTimeStamp = defaultCutOffTimeStamp;
    this.cutOffTimestampPerRef =
        cutOffTimestampPerRef == null ? Collections.emptyMap() : cutOffTimestampPerRef;
  }

  public IdentifiedResult identifyExpiredContents(SparkSession session) {
    // a) Algorithm for identifying the live contents and return the bloom filter per content-id.
    //    Walk through each live reference distributively (one spark task for each reference).
    //    For each commit in a reference, collect dropped table keys if present.
    //    If the commit is head commit, get all keys and add its contents to bloom filter.
    //    Else If for the keys that are NOT in dropped table keys AND commit is not expired (based
    //      on cutoff time of this reference). Add its content to bloom filter.
    //    Else if the content is expired, Stop traversing the commits
    //      as there cannot be any more live commits for this reference.
    //    Collect bloom filter per content id from each task and merge them.
    try (NessieApiV1 api = DistributedIdentifyUtil.getApi(configuration)) {
      List<Reference> references = api.getAllReferences().get().getReferences();
      DistributedIdentifyUtil distributedIdentifyUtil =
          new DistributedIdentifyUtil(session, configuration);
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap =
          distributedIdentifyUtil.getLiveContentsBloomFilters(
              references, cutOffTimestampPerRef, defaultCutOffTimeStamp);

      // b) Algorithm for identifying the expired and live contents
      //      and return the list of expired contents and list of live contents per content id.
      //    Get the list of dead reference from reflog
      //      and append it to current list of live references.
      //    Walk through each reference distributively (one spark task for each reference).
      //    For each commit in a reference,
      //      get the contents (use DETACHED reference here as some references are dead).
      //    Check if the content is expired by checking against the bloom filter.
      //    Merge the result of each task and return the result.
      List<Reference> deadReferences = collectDeadReferences(api);
      List<Reference> allRefs = new ArrayList<>(references);
      if (deadReferences.size() > 0) {
        allRefs.addAll(deadReferences);
      }
      return distributedIdentifyUtil.getIdentifiedResults(liveContentsBloomFilterMap, allRefs);
    }
  }

  private List<Reference> collectDeadReferences(NessieApiV1 api) {
    Stream<RefLogResponse.RefLogResponseEntry> reflogStream;
    try {
      reflogStream =
          StreamingUtil.getReflogStream(
              api,
              null,
              null,
              "reflog.operation == 'DELETE_REFERENCE' || reflog.operation == "
                  + "'ASSIGN_REFERENCE'",
              OptionalInt.empty());
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    return reflogStream
        .map(
            entry -> {
              String hash;
              switch (entry.getOperation()) {
                case "DELETE_REFERENCE":
                  hash = entry.getCommitHash();
                  break;
                case "ASSIGN_REFERENCE":
                  hash = entry.getSourceHashes().get(0);
                  break;
                default:
                  throw new RuntimeException(
                      entry.getOperation() + " operation found in dead reflog query");
              }
              switch (entry.getRefType()) {
                case RefLogResponse.RefLogResponseEntry.BRANCH:
                  return Branch.of(entry.getRefName(), hash);
                case RefLogResponse.RefLogResponseEntry.TAG:
                  return Tag.of(entry.getRefName(), hash);
                default:
                  throw new RuntimeException(
                      entry.getRefType() + " type reference is found in dead reflog query");
              }
            })
        .collect(Collectors.toList());
  }
}
