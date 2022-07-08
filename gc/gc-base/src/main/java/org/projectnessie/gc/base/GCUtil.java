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
package org.projectnessie.gc.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

public final class GCUtil {

  private GCUtil() {}

  private static final ObjectMapper objectMapper = new ObjectMapper();

  /** Serialize {@link Reference} object using JSON Serialization. */
  public static String serializeReference(Reference reference) {
    try {
      return objectMapper.writeValueAsString(reference);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /** Deserialize JSON String to {@link Reference} object. */
  public static Reference deserializeReference(String reference) {
    try {
      return objectMapper.readValue(reference, Reference.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Instant getInstantFromMicros(long microsSinceEpoch) {
    return Instant.ofEpochSecond(
        TimeUnit.MICROSECONDS.toSeconds(microsSinceEpoch),
        TimeUnit.MICROSECONDS.toNanos(
            Math.floorMod(microsSinceEpoch, TimeUnit.SECONDS.toMicros(1))));
  }

  /**
   * Get the branch if exists based on branch name. If not exists, creates a new branch pointing to
   * the beginning of time (NO_ANCESTOR hash)
   */
  public static void getOrCreateEmptyBranch(NessieApiV1 api, String branchName) {
    try {
      api.getReference().refName(branchName).get();
    } catch (NessieNotFoundException e) {
      // create a gc branch pointing to NO_ANCESTOR hash.
      try {
        api.createReference().reference(Branch.of(branchName, null)).create();
      } catch (NessieNotFoundException | NessieConflictException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Builds the client builder; default({@link HttpClientBuilder}) or custom, based on the
   * configuration provided.
   *
   * @param configuration map of client builder configurations.
   * @return {@link NessieApiV1} object.
   */
  public static NessieApiV1 getApi(Map<String, String> configuration) {
    String clientBuilderClassName =
        configuration.get(NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL);
    NessieClientBuilder<?> builder;
    if (clientBuilderClassName == null) {
      // Use the default HttpClientBuilder
      builder = HttpClientBuilder.builder();
    } else {
      // Use the custom client builder
      try {
        builder =
            Class.forName(clientBuilderClassName)
                .asSubclass(NessieClientBuilder.class)
                .getDeclaredConstructor()
                .newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(
            String.format(
                "No custom client builder class found for '%s' ", clientBuilderClassName));
      } catch (InvocationTargetException
          | InstantiationException
          | IllegalAccessException
          | NoSuchMethodException e) {
        throw new IllegalArgumentException(
            String.format("Could not initialize '%s': ", clientBuilderClassName), e);
      }
    }
    return builder.fromConfig(configuration::get).build(NessieApiV1.class);
  }
}
