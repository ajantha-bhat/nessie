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
package org.projectnessie.gc.base;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** Config params for GC. */
@Value.Immutable
public interface GCParams extends Serializable {

  /** Nessie client configurations from {@link org.projectnessie.client.NessieConfigConstants}. */
  Map<String, String> getNessieClientConfigs();

  /** Optional cutoff time per live reference. */
  @Nullable
  Map<String, Instant> getCutOffTimestampPerRef();

  /** Default cutoff time for all the references. */
  Instant getDefaultCutOffTimestamp();

  /** Optional cutoff time for all the dead references. */
  @Nullable
  Instant getDeadReferenceCutOffTimeStamp();

  /**
   * Optional spark partitions count to be used for distributing references. Default total reference
   * count (live + dead) will be used.
   */
  @Nullable
  Integer getSparkPartitionsCount();

  /**
   * Commit protection duration to avoid expiring on going or recent commits. Default is 2 hours.
   */
  @Value.Default
  default Duration getCommitProtectionDuration() {
    // default is kept as 2 hours.
    return Duration.ofHours(2);
  }

  /**
   * Optional bloom filter expected live commits entries per reference. Default is total commits in
   * the default reference.
   */
  @Nullable
  Long getBloomFilterExpectedEntries();

  /** Optional bloom filter fpp. Default value is 0.03d. */
  @Value.Default
  default double getBloomFilterFpp() {
    // default value is kept same as underlying Guava bloom filter default fpp.
    return 0.03d;
  }

  /** Nessie catalog name to be used with spark to create the output results table. */
  String getNessieCatalogName();

  /**
   * Branch's name to be used for creating the output table.
   *
   * <p>If the branch doesn't exist for this name, branch with this name pointing to beginning of
   * time (aka NO_ANCESTOR hash) will be created.
   */
  String getOutputBranchName();

  /**
   * Output table identifier (namespace and table name) to be used for storing the results in {@link
   * #getOutputBranchName()}.
   */
  String getOutputTableIdentifier();

  @Value.Check
  default void validate() {
    Integer taskCount = getSparkPartitionsCount();
    if (taskCount != null && taskCount <= 0) {
      throw new IllegalArgumentException("taskCount has invalid value: " + taskCount);
    }
    Duration commitProtectionDuration = getCommitProtectionDuration();
    if (commitProtectionDuration.isNegative()) {
      throw new IllegalArgumentException(
          "commitProtectionDuration has invalid value: " + commitProtectionDuration);
    }
    Long bloomFilterExpectedEntries = getBloomFilterExpectedEntries();
    if (bloomFilterExpectedEntries != null && bloomFilterExpectedEntries < 0) {
      throw new IllegalArgumentException(
          "bloomFilterExpectedEntries has invalid value: " + bloomFilterExpectedEntries);
    }
    double bloomFilterFpp = getBloomFilterFpp();
    if (!(bloomFilterFpp > 0.0d && bloomFilterFpp < 1.0d)) {
      throw new IllegalArgumentException("bloomFilterFpp has invalid value: " + bloomFilterFpp);
    }
  }
}
