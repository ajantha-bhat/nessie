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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

/** Base class for an object stored within Nessie. */
@Schema(
    type = SchemaType.OBJECT,
    title = "Content",
    oneOf = {IcebergTable.class, DeltaLakeTable.class, IcebergView.class, Namespace.class},
    discriminatorMapping = {
      @DiscriminatorMapping(value = "ICEBERG_TABLE", schema = IcebergTable.class),
      @DiscriminatorMapping(value = "DELTA_LAKE_TABLE", schema = DeltaLakeTable.class),
      @DiscriminatorMapping(value = "ICEBERG_VIEW", schema = IcebergView.class),
      @DiscriminatorMapping(value = "NAMESPACE", schema = Namespace.class)
    },
    discriminatorProperty = "type")
@JsonSubTypes({
  @Type(IcebergTable.class),
  @Type(DeltaLakeTable.class),
  @Type(IcebergView.class),
  @Type(Namespace.class)
})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class Content {

  public enum Type {
    UNKNOWN,
    ICEBERG_TABLE,
    DELTA_LAKE_TABLE,
    ICEBERG_VIEW,
    NAMESPACE
  }

  /**
   * Unique id for this object.
   *
   * <p>This id is unique for the entire lifetime of this Content object and persists across
   * renames. Two content objects with the same key will have different id.
   */
  @Value.Default
  public String getId() {
    return UUID.randomUUID().toString();
  }

  /**
   * Returns the {@link Type} enum constant for this content object.
   *
   * <p>The name of the returned enum value should match the JSON type name used for serializing the
   * content object.
   */
  @Value.Redacted
  @JsonIgnore
  public abstract Type getType();

  /**
   * Unwrap object if possible, otherwise throw.
   *
   * @param <T> Type to wrap to.
   * @param clazz Class we're trying to return.
   * @return The return value
   */
  public <T> Optional<T> unwrap(Class<T> clazz) {
    Objects.requireNonNull(clazz);
    if (clazz.isAssignableFrom(getClass())) {
      return Optional.of(clazz.cast(this));
    }
    return Optional.empty();
  }
}
