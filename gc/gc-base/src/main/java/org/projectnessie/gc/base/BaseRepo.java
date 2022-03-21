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

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.model.ImmutableTableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseRepo {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseRepo.class);

  final SparkSession sparkSession;
  final String catalogAndTableWithRefName;

  public BaseRepo(
      SparkSession sparkSession,
      String catalog,
      String gcRefName,
      String gcCheckPointTableIdentifier) {
    this.sparkSession = sparkSession;
    this.catalogAndTableWithRefName = withRefName(catalog, gcCheckPointTableIdentifier, gcRefName);
  }

  static void createTableIfAbsent(
      SparkSession sparkSession,
      String catalogName,
      TableIdentifier tableIdentifier,
      String gcRefName,
      Schema icebergSchema) {
    try {
      GCUtil.loadNessieCatalog(sparkSession, catalogName, gcRefName)
          .createTable(tableIdentifier, icebergSchema);
    } catch (AlreadyExistsException ex) {
      // Table can exist from previous GC run, no need to throw exception.
    }
  }

  private static String withRefName(String catalog, String identifier, String refName) {
    int tableNameIndex = identifier.lastIndexOf(".");
    String namespace = identifier.substring(0, tableNameIndex);
    String tableName = identifier.substring(tableNameIndex + 1);
    return catalog
        + "."
        + namespace
        + "."
        + ImmutableTableReference.builder().name(tableName).reference(refName).build();
  }

  Dataset<Row> sql(String sqlStatement, Object... args) {
    String sql = String.format(sqlStatement, args);
    LOGGER.debug("Executing the sql -> {}", sql);
    return sparkSession.sql(sql);
  }
}
