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

import java.util.Set;
import org.agrona.collections.LongHashSet;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;

public final class IcebergExpiredContentValues extends ExpiredContentValues {

  // The "missingValue" for org.agrona.collections.LongHashSet is -1L, which matches the
  // "no snapshot ID" value in Iceberg.
  private final LongHashSet expiredSnapshotIds = new LongHashSet();
  private final LongHashSet liveSnapshotIds = new LongHashSet();

  @Override
  protected void addValue(Content content, boolean isExpired) {
    IcebergTable icebergTable = (IcebergTable) content;
    long snapshotId = icebergTable.getSnapshotId();

    // -1 means "no snapshot" in Iceberg terms.
    if (snapshotId != -1L) {
      if (isExpired) {
        // If the snapshot is added as live from other ref, then it cannot be moved to expired
        // state.
        if (!liveSnapshotIds.contains(snapshotId)) {
          expiredSnapshotIds.add(snapshotId);
        }
      } else {
        liveSnapshotIds.add(snapshotId);
        // If snapshot is found live, remove it from expired set if exists.
        expiredSnapshotIds.remove(snapshotId);
      }
    }
  }

  public Set<Long> getExpiredSnapshotIds() {
    return expiredSnapshotIds;
  }

  public LongHashSet getLiveSnapshotIds() {
    return liveSnapshotIds;
  }
}
