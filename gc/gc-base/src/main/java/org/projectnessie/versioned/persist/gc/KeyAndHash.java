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

import org.immutables.value.Value;
import org.projectnessie.model.ContentsKey;

@Value.Immutable
@Value.Style(jdkOnly = true)
public interface KeyAndHash {
  ContentsKey getKey();

  String getHash();

  static KeyAndHash of(ContentsKey key, String hash) {
    return ImmutableKeyAndHash.builder().key(key).hash(hash).build();
  }
}
