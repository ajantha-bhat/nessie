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
package org.apache.iceberg;

import java.util.Collections;

/**
 * Custom metadata builder as {@link TableMetadata} has package private constructor and other public
 * constructor will not use new UUID.
 */
public final class GcMetadataUtil {

  public static TableMetadata getMetadataWithoutHistory(TableMetadata base) {
    // metadata file without snapshots, snapshotHistory, previousFiles and metadataUpdates.
    return new TableMetadata(
        base.metadataFileLocation(),
        base.formatVersion(),
        base.uuid(),
        base.location(),
        0,
        base.lastUpdatedMillis(),
        base.lastColumnId(),
        base.currentSchemaId(),
        base.schemas(),
        base.defaultSpecId(),
        base.specs(),
        base.lastAssignedPartitionId(),
        base.defaultSortOrderId(),
        base.sortOrders(),
        base.properties(),
        base.currentSnapshot().snapshotId(),
        base.snapshots(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
  }
}
