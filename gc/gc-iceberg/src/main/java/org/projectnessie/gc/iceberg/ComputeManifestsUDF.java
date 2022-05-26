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

import com.esotericsoftware.minlog.Log;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.sql.api.java.UDF1;

class ComputeManifestsUDF implements UDF1<String, List<String>> {

  private final FileIO io;

  public ComputeManifestsUDF(FileIO io) {
    this.io = io;
  }

  @Override
  public List<String> call(String metadataLocation) {
    TableMetadata metadata = TableMetadataParser.read(io, metadataLocation);
    List<String> manifestFiles = new ArrayList<>();
    metadata
        .snapshots()
        .forEach(
            snapshot -> {
              try {
                manifestFiles.addAll(
                    snapshot.allManifests().stream()
                        .map(ManifestFile::path)
                        .collect(Collectors.toList()));
              } catch (NotFoundException e) {
                // As checkpoint is till the last live commit, there can be some expired commits in
                // between.
                // So, when expired is called for the second time or later, manifestlists will not
                // be present for
                // previously expired contents. Hence, ignore those snapshots.
                Log.warn(e.getMessage());
              }
            });
    return manifestFiles;
  }
}
