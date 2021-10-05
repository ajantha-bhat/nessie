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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.Reference;

/** Global commit-log scanning state and result. */
public final class ContentsValuesCollector<GC_CONTENTS_VALUES extends ContentsValues> {

  private final Supplier<GC_CONTENTS_VALUES> newContentsValues;

  /** Contents per content-id. */
  final Map<String, GC_CONTENTS_VALUES> contentsValues = new ConcurrentHashMap<>();

  public ContentsValuesCollector(Supplier<GC_CONTENTS_VALUES> newContentsValues) {
    this.newContentsValues = newContentsValues;
  }

  public GC_CONTENTS_VALUES contentsValues(String contentsId) {
    return contentsValues.computeIfAbsent(contentsId, k -> newContentsValues.get());
  }

  public void gotValue(
      Contents contents, Reference reference, ContentsKey contentsKey, boolean isLive) {
    contentsValues(contents.getId()).gotValue(contents, reference, contentsKey, isLive);
  }
}
