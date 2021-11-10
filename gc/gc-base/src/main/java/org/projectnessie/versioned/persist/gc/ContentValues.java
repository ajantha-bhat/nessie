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

import java.util.HashMap;
import java.util.Map;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

/**
 * Contains details about one content objects: the content-type, the live values and the non-live
 * values.
 */
public abstract class ContentValues {

  /** A reference via which this content object (table) is reachable. */
  private final Map<String, KeyAndHash> referencesToKeyAndHash = new HashMap<>();

  public Map<String, KeyAndHash> getReferencesToKeyAndHash() {
    return referencesToKeyAndHash;
  }

  private void setLiveAtIfAbsent(Reference reference, ContentKey key) {
    referencesToKeyAndHash.putIfAbsent(
        reference.getName(), KeyAndHash.of(key, reference.getHash()));
  }

  void gotValue(Content content, Reference reference, ContentKey key, boolean isLive) {
    synchronized (this) {
      // Remove potentially recorded non-live values. E.g. renaming a table writes both a put+delete
      // operation.
      addValue(content, isLive);

      setLiveAtIfAbsent(reference, key);
    }
  }

  protected abstract void addValue(Content content, boolean isLive);

  @Override
  public String toString() {
    return "ContentValues{" + ", liveViaKeyInReferences=" + referencesToKeyAndHash + '}';
  }
}
