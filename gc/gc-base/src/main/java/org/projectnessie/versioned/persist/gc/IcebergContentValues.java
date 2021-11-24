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

public final class IcebergContentValues extends ContentValues {

  // The "missingValue" for org.agrona.collections.LongHashSet is -1L, which matches the
  // "no snapshot ID" value in Iceberg.
  private final LongHashSet snapshotIds = new LongHashSet();
  private final LongHashSet nonLiveSnapshotIds = new LongHashSet();

  @Override
  protected void addValue(Content content, boolean isLive) {
    IcebergTable icebergTable = (IcebergTable) content;
    long snapshotId = icebergTable.getSnapshotId();

    // -1 means "no snapshot"
    if (snapshotId != -1L) {
      if (isLive) {
        snapshotIds.add(snapshotId);
        nonLiveSnapshotIds.remove(snapshotId);
      } else {
        if (!snapshotIds.contains(snapshotId)) {
          nonLiveSnapshotIds.add(snapshotId);
        }
      }
    }
  }

  public Set<Long> getLiveSnapshotIds() {
    return snapshotIds;
  }

  public LongHashSet getNonLiveSnapshotIds() {
    return nonLiveSnapshotIds;
  }
}
