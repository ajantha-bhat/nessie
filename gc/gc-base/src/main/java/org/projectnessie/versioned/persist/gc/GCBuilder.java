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
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Reference;

final class GCBuilder implements GC.Builder {

  private NessieApiV1 api;
  private Instant defaultCutOffTimeStamp;
  private final List<Function<Reference, Instant>> cutOffTimeStampComputations = new ArrayList<>();

  @Override
  public GC.Builder withApi(NessieApiV1 api) {
    this.api = api;
    return this;
  }

  @Override
  public GC.Builder withDefaultCutOffTimeStamp(Instant defaultCutOffTimeStamp) {
    this.defaultCutOffTimeStamp = defaultCutOffTimeStamp;
    return this;
  }

  @Override
  public GC.Builder addCutOffTimeStampPerReferenceComputation(
      Function<Reference, Instant> cutOffTimeStampPerReferenceComputation) {
    this.cutOffTimeStampComputations.add(cutOffTimeStampPerReferenceComputation);
    return this;
  }

  @Override
  public GC build() {
    Instant defaultCutOffTimeStamp =
        Objects.requireNonNull(
            this.defaultCutOffTimeStamp, "defaultCutOffTimeStamp must not be null");

    Function<Reference, Instant> cutOffTimestamp;
    if (cutOffTimeStampComputations.isEmpty()) {
      cutOffTimestamp = ref -> defaultCutOffTimeStamp;
    } else {
      List<Function<Reference, Instant>> cutOffTimeStampComputations =
          this.cutOffTimeStampComputations;
      cutOffTimestamp =
          ref -> {
            Instant computed = null;
            for (Function<Reference, Instant> pruneBeforeCompute : cutOffTimeStampComputations) {
              Instant pruneBefore = pruneBeforeCompute.apply(ref);
              if (pruneBefore != null
                  && (computed == null || computed.compareTo(pruneBefore) > 0)) {
                computed = pruneBefore;
              }
            }
            if (computed == null) {
              computed = defaultCutOffTimeStamp;
            }
            return computed;
          };
    }

    return new GCImpl(api, cutOffTimestamp);
  }
}
