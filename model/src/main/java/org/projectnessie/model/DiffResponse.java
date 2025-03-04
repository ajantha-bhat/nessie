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
package org.projectnessie.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(type = SchemaType.OBJECT, title = "DiffResponse")
@Value.Immutable
@JsonSerialize(as = ImmutableDiffResponse.class)
@JsonDeserialize(as = ImmutableDiffResponse.class)
public interface DiffResponse extends PaginatedResponse {

  List<DiffEntry> getDiffs();

  @Value.Immutable
  @JsonSerialize(as = ImmutableDiffEntry.class)
  @JsonDeserialize(as = ImmutableDiffEntry.class)
  interface DiffEntry {
    @Value.Parameter(order = 1)
    ContentKey getKey();

    @Nullable
    @Value.Parameter(order = 2)
    @SuppressWarnings("immutables:from")
    Content getFrom();

    @Nullable
    @Value.Parameter(order = 3)
    Content getTo();

    static DiffEntry diffEntry(ContentKey key, Content from) {
      return diffEntry(key, from, null);
    }

    static DiffEntry diffEntry(ContentKey key, Content from, Content to) {
      return ImmutableDiffEntry.of(key, from, to);
    }
  }
}
