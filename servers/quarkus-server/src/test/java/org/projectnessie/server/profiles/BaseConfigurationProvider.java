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
package org.projectnessie.server.profiles;

import com.google.common.collect.ImmutableMap;

public abstract class BaseConfigurationProvider {

  protected static ImmutableMap.Builder<String, String> basicTestConfigurations() {
    return addBasicTestConfigurations(ImmutableMap.builder());
  }

  protected static ImmutableMap.Builder<String, String> addBasicTestConfigurations(
      ImmutableMap.Builder<String, String> builder) {
    return builder
        .put("quarkus.jaeger.sampler-type", "const")
        .put("quarkus.test.native-image-profile", "test")
        .put("quarkus.http.auth.basic", "true")
        .put("quarkus.security.users.embedded.enabled", "true")
        .put("quarkus.security.users.embedded.plain-text", "true")
        .put("quarkus.security.users.embedded.users.admin_user", "test123")
        .put("quarkus.security.users.embedded.users.test_user", "test_user")
        .put("quarkus.security.users.embedded.roles.admin_user", "admin,user")
        .put("quarkus.security.users.embedded.roles.test_user", "test123")
        .put("nessie.version.store.advanced.key-prefix", "nessie-test")
        .put("nessie.version.store.advanced.commit-retries", "42")
        .put("nessie.version.store.advanced.tx.batch-size", "41");
  }
}
