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
package org.projectnessie.gc.base;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/** Contains details about one content id: like expired contents. */
public class ContentValues implements Serializable {

  private static final long serialVersionUID = -1268447510930923804L;
  // Note that we may need more info like refName, hashOnRef, contentKey
  // for the consumer of GC along with expired contents.
  // Hence, a separate class.
  // As these values will be written as a Spark Row of GC output table,
  // keeping it as a JSON serialized content objects.
  private final Set<String> expiredContents = new HashSet<>();

  void gotValue(String content) {
    expiredContents.add(content);
  }

  public Set<String> getExpiredContents() {
    return expiredContents;
  }
}
