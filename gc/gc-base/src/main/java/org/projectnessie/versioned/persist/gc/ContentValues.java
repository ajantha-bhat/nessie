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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

/** Contains details about one content object: key, the live values and the expired values. */
public class ContentValues implements Serializable {

  private static final long serialVersionUID = -1268447510930923804L;
  private ContentKey contentKey;
  private final Set<Content> expiredContents = new HashSet<>();
  private final Set<Content> liveContents = new HashSet<>();

  public ContentKey getContentKey() {
    return contentKey;
  }

  private void setIfAbsent(ContentKey key) {
    if (this.contentKey == null) {
      this.contentKey = key;
    }
  }

  void gotValue(Content content, ContentKey key, boolean isExpired) {
    synchronized (this) {
      addValue(content, isExpired);
      setIfAbsent(key);
    }
  }

  private void addValue(Content content, boolean isExpired) {
    if (isExpired) {
      expiredContents.add(content);
    } else {
      liveContents.add(content);
    }
  }

  public Set<Content> getExpiredContents() {
    return expiredContents;
  }

  public Set<Content> getLiveContents() {
    return liveContents;
  }
}
