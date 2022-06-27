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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.gc.iceberg.ExpireContentsProcedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class with same package as Iceberg because {@link ManifestFiles#open(ManifestFile, FileIO)}
 * is package private.
 */
public final class GcMetadataUtil {

  private static final Logger LOG = LoggerFactory.getLogger(GcMetadataUtil.class);

  public static void computeManifestsAndDataFiles(
      Snapshot snapshot, FileIO io, List<String> allFiles) {
    List<ManifestFile> manifestFiles = new ArrayList<>();
    try {
      manifestFiles.addAll(snapshot.allManifests());
    } catch (NotFoundException e) {
      // As checkpoint is till the last live commit, there can be some expired commits in
      // between.
      // So, when expired is called for the second time or later, manifestlists will not
      // be present for
      // previously expired contents. Hence, ignore those snapshots.
      LOG.warn("Failed to read manifestLists: {}", e.getMessage());
    }

    List<String> manifests =
        manifestFiles.stream()
            .map(
                manifest ->
                    ExpireContentsProcedure.FileType.ICEBERG_MANIFEST.name()
                        + "#"
                        + manifest.path())
            .collect(Collectors.toList());
    allFiles.addAll(manifests);

    List<String> dataFiles = new ArrayList<>();
    manifestFiles.forEach(
        manifest -> {
          try (ManifestReader<?> reader = ManifestFiles.open(manifest, io)) {
            List<String> files =
                StreamSupport.stream(reader.entries().spliterator(), false)
                    .map(
                        entry ->
                            ExpireContentsProcedure.FileType.DATA_FILE.name()
                                + "#"
                                + entry.file().path().toString())
                    .collect(Collectors.toList());
            dataFiles.addAll(files);
          } catch (IOException | UncheckedIOException e) {
            LOG.warn("Failed to read the Manifests: {}", e.getMessage());
          }
        });
    allFiles.addAll(dataFiles);
  }
}
