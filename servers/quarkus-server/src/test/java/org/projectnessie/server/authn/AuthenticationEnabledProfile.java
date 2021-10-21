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
package org.projectnessie.server.authn;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;
import org.projectnessie.server.profiles.BaseConfigurationProvider;

/** A simple {@link QuarkusTestProfile} that enables Nessie authentication. */
public class AuthenticationEnabledProfile extends BaseConfigurationProvider
    implements QuarkusTestProfile {

  public static final Map<String, String> CONFIG_OVERRIDES =
      basicTestConfigurations().put("nessie.server.authentication.enabled", "true").build();

  @Override
  public Map<String, String> getConfigOverrides() {
    return CONFIG_OVERRIDES;
  }
}
