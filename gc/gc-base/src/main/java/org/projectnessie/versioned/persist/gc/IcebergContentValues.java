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

import java.util.LinkedHashSet;
import java.util.Set;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;

public final class IcebergContentValues extends ContentValues {

  private final Set<String> metadataPointers = new LinkedHashSet<>();
  private final Set<String> nonLiveMetadataPointers = new LinkedHashSet<>();

  @Override
  protected void addValue(Content content, boolean isLive) {
    IcebergTable icebergTable = (IcebergTable) content;
    String metadataPointer = icebergTable.getMetadataLocation();

    if (isLive) {
      metadataPointers.add(metadataPointer);
      nonLiveMetadataPointers.remove(metadataPointer);
    } else {
      if (!metadataPointers.contains(metadataPointer)) {
        nonLiveMetadataPointers.add(metadataPointer);
      }
    }
  }

  public Set<String> getLiveMetadataPointers() {
    return metadataPointers;
  }

  public Set<String> getNonLiveMetadataPointers() {
    return nonLiveMetadataPointers;
  }
}
