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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.GcMetadataUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.sql.api.java.UDF1;

class ComputeExpiredFilesUDF implements UDF1<String, List<String>> {

  private final FileIO io;
  private final Set<String> expiredManifests;

  public ComputeExpiredFilesUDF(FileIO io, Set<String> expiredManifests) {
    this.io = io;
    this.expiredManifests = expiredManifests;
  }

  @Override
  public List<String> call(String metadataLocation) {
    TableMetadata metadata = TableMetadataParser.read(io, metadataLocation);
    List<String> expiredFiles = new ArrayList<>();
    List<String> manifestLists =
        metadata.snapshots().stream()
            .map(
                snapshot ->
                    ExpireContentsProcedure.ExpiredContentType.ICEBERG_MANIFESTLIST.name()
                        + "#"
                        + snapshot.manifestListLocation())
            .collect(Collectors.toList());
    List<String> dataFiles = GcMetadataUtil.computeDataFiles(metadata, io, expiredManifests);
    // manifestLists followed by manifests followed by datafiles.
    expiredFiles.addAll(manifestLists);
    expiredFiles.addAll(
        expiredManifests.stream()
            .map(
                manifest ->
                    ExpireContentsProcedure.ExpiredContentType.ICEBERG_MANIFEST.name()
                        + "#"
                        + manifest)
            .collect(Collectors.toList()));
    expiredFiles.addAll(dataFiles);
    return expiredFiles;
  }
}
