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
import org.apache.iceberg.GcMetadataUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ComputeAllFilesUDF implements UDF2<String, Long, List<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(ComputeAllFilesUDF.class);

  private final FileIO io;

  public ComputeAllFilesUDF(FileIO io) {
    this.io = io;
  }

  @Override
  public List<String> call(String metadataLocation, Long snapshotId) {
    List<String> allFiles = new ArrayList<>();
    TableMetadata metadata = TableMetadataParser.read(io, metadataLocation);
    Snapshot snapshot = metadata.snapshot(snapshotId);
    if (snapshot == null) {
      LOG.warn("Unable to find snapshot {} in {}", snapshotId, metadataLocation);
      return allFiles;
    }
    // manifestLists followed by manifests followed by datafiles.
    allFiles.add(
        ExpireContentsProcedure.FileType.ICEBERG_MANIFESTLIST.name()
            + "#"
            + snapshot.manifestListLocation());
    GcMetadataUtil.computeManifestsAndDataFiles(snapshot, io, allFiles);
    return allFiles;
  }
}
