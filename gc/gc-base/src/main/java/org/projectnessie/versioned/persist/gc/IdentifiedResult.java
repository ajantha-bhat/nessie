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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

public class IdentifiedResult implements Serializable {

  private static final long serialVersionUID = 900739110223735484L;
  // ContentValues per content id.
  private final Map<String, ContentValues> contentValues = new ConcurrentHashMap<>();

  public void addContent(Content content, ContentKey contentKey, boolean isExpired) {
    contentValues
        .computeIfAbsent(content.getId(), k -> new ContentValues())
        .gotValue(content, contentKey, isExpired);
  }

  public void addNewContentValues(Map<String, ContentValues> newContentValues) {
    newContentValues.forEach(
        (key, value) -> {
          value
              .getExpiredContents()
              .forEach(content -> addContent(content, value.getContentKey(), true));

          value
              .getLiveContents()
              .forEach(content -> addContent(content, value.getContentKey(), false));
        });
  }

  public Map<String, ContentValues> getContentValues() {
    return contentValues;
  }
}
