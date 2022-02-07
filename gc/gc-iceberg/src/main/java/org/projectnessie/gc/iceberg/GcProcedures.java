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
package org.projectnessie.gc.iceberg;

import java.util.Arrays;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

final class GcProcedures {

  public static final String NAMESPACE = "nessie_gc";
  static final String[] NAMESPACE_ARRAY = {NAMESPACE};

  static boolean isGcNamespace(Identifier identifier) {
    return Arrays.equals(NAMESPACE_ARRAY, identifier.namespace());
  }

  static AbstractGcProcedure loadGcProcedure(Identifier identifier, TableCatalog catalog)
      throws NoSuchProcedureException {
    switch (identifier.name()) {
      case IdentifyLiveSnapshotsProcedure.PROCEDURE_NAME:
        return new IdentifyLiveSnapshotsProcedure(catalog);
      case ExpireSnapshotsProcedure.PROCEDURE_NAME:
        return new ExpireSnapshotsProcedure(catalog);
      default:
        throw new NoSuchProcedureException(identifier);
    }
  }
}
