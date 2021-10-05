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
import java.util.function.Function;
import java.util.function.Predicate;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Reference;

public interface GC {
  static Builder builder() {
    return new GCBuilder();
  }

  <GC_CONTENTS_VALUES extends ContentsValues> GCResult<GC_CONTENTS_VALUES> performGC(
      ContentsValuesCollector<GC_CONTENTS_VALUES> contentsValuesCollector);

  interface Builder {
    Builder withApi(NessieApiV1 api);

    /**
     * Commits that are older than the specified {@code defaultLiveAfterValue} will be considered as
     * eligible for GC.
     *
     * <p>This is effectively a shortcut for {@link #addLiveAfterComputation(Function)
     * addPruneBeforeCompute(ref -> defaultPruneBefore)}.
     *
     * @see #addLiveAfterComputation(Function)
     */
    Builder withDefaultLiveAfterValue(Instant defaultLiveAfterValue);

    /**
     * Adds a function to compute the timestamp after which commits for a named reference are
     * considered as "live". The <em>lowest/smallest</em> {@link Instant} returned by all {@code
     * pruneBeforeComputation} functions will be used. The return value {@code null} from a {@code
     * pruneBeforeComputation} means it has "no opinion" on that named reference.
     *
     * @see #withDefaultLiveAfterValue(Instant)
     */
    Builder addLiveAfterComputation(Function<Reference, Instant> liveAfterComputation);

    /**
     * Optional predicate to only perform GC for certain content type.
     *
     * <p>GC works for all content-types by default.
     */
    Builder withContentTypeInclusionPredicate(
        Predicate<Contents.Type> contentTypeInclusionPredicate);

    /**
     * Consider all commits on all references that have a commit-timestamp newer than the given
     * value.
     *
     * <p>Callers should set this to a value that is somewhat "older" than the last "identify
     * garbage" (GC) run.
     *
     * <p>Consecutive "identify garbage" (GC) runs shall not only identify live contents but also
     * contents that became no longer accessible.
     */
    Builder withReadUntilCommitTimestamp(Instant readUntilCommitTimestamp);

    /** Build the {@link GC} instance. */
    GC build();
  }
}
