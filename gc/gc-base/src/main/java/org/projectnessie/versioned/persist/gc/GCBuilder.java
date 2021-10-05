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
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Reference;

final class GCBuilder implements GC.Builder {

  private NessieApiV1 api;
  private Instant defaultLiveAfterValue;
  private final List<Function<Reference, Instant>> liveAfterComputations = new ArrayList<>();
  private Predicate<Contents.Type> contentTypeInclusionPredicate;
  private Instant readUntilCommitTimestamp;

  @Override
  public GC.Builder withApi(NessieApiV1 api) {
    this.api = api;
    return this;
  }

  @Override
  public GC.Builder withDefaultLiveAfterValue(Instant defaultLiveAfterValue) {
    this.defaultLiveAfterValue = defaultLiveAfterValue;
    return this;
  }

  @Override
  public GC.Builder addLiveAfterComputation(Function<Reference, Instant> liveAfterComputation) {
    this.liveAfterComputations.add(liveAfterComputation);
    return this;
  }

  @Override
  public GC.Builder withContentTypeInclusionPredicate(
      Predicate<Contents.Type> contentTypeInclusionPredicate) {
    this.contentTypeInclusionPredicate = contentTypeInclusionPredicate;
    return this;
  }

  @Override
  public GC.Builder withReadUntilCommitTimestamp(Instant readUntilCommitTimestamp) {
    this.readUntilCommitTimestamp = readUntilCommitTimestamp;
    return this;
  }

  @Override
  public GC build() {
    Predicate<Contents.Type> contentTypeInclusion =
        Objects.requireNonNull(
            contentTypeInclusionPredicate, "contentTypeInclusionPredicate must be present");

    Instant defaultLiveAfterValue =
        Objects.requireNonNull(
            this.defaultLiveAfterValue, "defaultLiveAfterValue must not be null");

    Instant readUntilCommitTimestamp =
        this.readUntilCommitTimestamp != null ? this.readUntilCommitTimestamp : Instant.EPOCH;

    Function<Reference, Instant> cutOffTimestamp;
    if (liveAfterComputations.isEmpty()) {
      cutOffTimestamp = ref -> defaultLiveAfterValue;
    } else {
      List<Function<Reference, Instant>> liveAfterComputations = this.liveAfterComputations;
      cutOffTimestamp =
          ref -> {
            Instant computed = null;
            for (Function<Reference, Instant> pruneBeforeCompute : liveAfterComputations) {
              Instant pruneBefore = pruneBeforeCompute.apply(ref);
              if (pruneBefore != null
                  && (computed == null || computed.compareTo(pruneBefore) > 0)) {
                computed = pruneBefore;
              }
            }
            if (computed == null) {
              computed = defaultLiveAfterValue;
            }
            return computed;
          };
    }

    return new GCImpl(api, contentTypeInclusion, cutOffTimestamp, readUntilCommitTimestamp);
  }
}
