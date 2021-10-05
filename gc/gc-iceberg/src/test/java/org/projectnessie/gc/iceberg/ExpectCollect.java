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
package org.projectnessie.gc.iceberg;

class ExpectCollect {

  final String referenceName;
  final String tableName;
  final String contentId;
  int numLiveSnapshots;
  int numCollectedSnapshots;
  int numLiveMetadataPointers;

  ExpectCollect(
      String referenceName,
      String tableName,
      String contentId,
      int numLiveSnapshots,
      int numCollectedSnapshots,
      int numLiveMetadataPointers) {
    this.referenceName = referenceName;
    this.tableName = tableName;
    this.contentId = contentId;
    this.numLiveSnapshots = numLiveSnapshots;
    this.numCollectedSnapshots = numCollectedSnapshots;
    this.numLiveMetadataPointers = numLiveMetadataPointers;
  }

  @Override
  public String toString() {
    return "ExpectCollect{"
        + "referenceName='"
        + referenceName
        + "', tableName='"
        + tableName
        + "', contentId='"
        + contentId
        + "', numLiveSnapshots="
        + numLiveSnapshots
        + ", numCollectedSnapshots="
        + numCollectedSnapshots
        + ", numLiveMetadataPointers="
        + numLiveMetadataPointers
        + '}';
  }
}
