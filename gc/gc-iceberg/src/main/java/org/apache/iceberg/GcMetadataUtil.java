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

import com.esotericsoftware.minlog.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.gc.iceberg.ExpireContentsProcedure;

/**
 * Util class with same package as Iceberg because {@link ManifestFiles#open(ManifestFile, FileIO)}
 * is package private.
 */
public final class GcMetadataUtil {

  public static List<String> computeDataFiles(
      TableMetadata metadata, FileIO io, Set<String> expiredManifests) {
    List<ManifestFile> expiredManifestFiles = new ArrayList<>();
    metadata
        .snapshots()
        .forEach(
            snapshot -> {
              try {
                expiredManifestFiles.addAll(
                    snapshot.allManifests().stream()
                        .filter(x -> expiredManifests.contains(x.path()))
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

    return expiredManifestFiles.stream()
        .flatMap(
            manifest -> {
              try (ManifestReader<?> reader = ManifestFiles.open(manifest, io)) {
                return StreamSupport.stream(reader.entries().spliterator(), false)
                    .map(
                        entry ->
                            ExpireContentsProcedure.ExpiredContentType.DATA_FILE.name()
                                + "#"
                                + entry.file().path().toString());
              } catch (IOException e) {
                throw new RuntimeException(
                    String.format("Failed to read manifest file: %s", manifest.path()), e);
              }
            })
        .collect(Collectors.toList());
  }
}
