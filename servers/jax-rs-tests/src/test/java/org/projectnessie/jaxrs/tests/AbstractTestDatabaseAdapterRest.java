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
package org.projectnessie.jaxrs.tests;

import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtensionForDatabaseAdapter;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
@ExtendWith(DatabaseAdapterExtension.class)
abstract class AbstractTestDatabaseAdapterRest extends AbstractRestSecurityContext {

  @NessieDbAdapter static DatabaseAdapter databaseAdapter;

  @RegisterExtension
  static org.projectnessie.jaxrs.ext.NessieJaxRsExtension server =
      jaxRsExtensionForDatabaseAdapter(() -> databaseAdapter);
}
