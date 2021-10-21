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
package org.projectnessie.server;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class TestUtil {

  public static Map<String, String> getBasicTestConfigurations() {
    return ImmutableMap.<String, String>builder()
        .put("%test.quarkus.jaeger.sampler-type", "const")
        .put("%test.quarkus.test.native-image-profile", "test")
        .put("%test.quarkus.http.auth.basic", "true")
        .put("%test.quarkus.security.users.embedded.enabled", "true")
        .put("%test.quarkus.security.users.embedded.plain-text", "true")
        .put("%test.quarkus.security.users.embedded.users.admin_user", "test123")
        .put("%test.quarkus.security.users.embedded.users.test_user", "test_user")
        .put("%test.quarkus.security.users.embedded.roles.admin_user", "admin,user")
        .put("%test.quarkus.security.users.embedded.roles.test_user", "test123")
        .put("%test.nessie.version.store.advanced.key-prefix", "nessie-test")
        .put("%test.nessie.version.store.advanced.commit-retries", "42")
        .put("%test.nessie.version.store.advanced.tx.batch-size", "41")
        .build();
  }

  public static Map<String, String> getAuthzTestConfigurations() {
    return ImmutableMap.<String, String>builder()
        .put(
            "%test.nessie.server.authorization.rules.allow_all",
            "op in ['VIEW_REFERENCE','CREATE_REFERENCE','DELETE_REFERENCE',"
                + "'LIST_COMMITLOG','READ_ENTRIES','LIST_COMMIT_LOG','COMMIT_CHANGE_AGAINST_REFERENCE',"
                + "'ASSIGN_REFERENCE_TO_HASH','UPDATE_ENTITY','READ_ENTITY_VALUE','DELETE_ENTITY'] && role=='admin_user'")
        .put(
            "%test.nessie.server.authorization.rules.allow_branch_listing",
            "op=='VIEW_REFERENCE' && role.startsWith('test_user') && ref.matches('.*')")
        .put(
            "%test.nessie.server.authorization.rules.allow_branch_creation",
            "op=='CREATE_REFERENCE' && role.startsWith('test_user') && ref.startsWith('allowedBranch')")
        .put(
            "%test.nessie.server.authorization.rules.allow_branch_deletion",
            "op=='DELETE_REFERENCE' && role.startsWith('test_user') && ref.startsWith('allowedBranch')")
        .put(
            "%test.nessie.server.authorization.rules.allow_listing_commitlog",
            "op=='LIST_COMMIT_LOG' && role.startsWith('test_user') && ref.startsWith('allowedBranch')")
        .put(
            "%test.nessie.server.authorization.rules.allow_entries_reading",
            "op=='READ_ENTRIES' && role.startsWith('test_user') && ref.startsWith('allowedBranch')")
        .put(
            "%test.nessie.server.authorization.rules.allow_assigning_ref_to_hash",
            "op=='ASSIGN_REFERENCE_TO_HASH' && role.startsWith('test_user') && ref.startsWith('allowedBranch')")
        .put(
            "%test.nessie.server.authorization.rules.allow_commits",
            ("op=='COMMIT_CHANGE_AGAINST_REFERENCE' && role.startsWith('test_user') && ref.startsWith"
                + "('allowedBranch'))"))
        .put(
            "%test.nessie.server.authorization.rules.allow_reading_entity_value",
            "op in ['VIEW_REFERENCE', 'READ_ENTITY_VALUE'] && role=='test_user' && path.startsWith('allowed.') "
                + "&& ref.startsWith('allowedBranch')")
        .put(
            "%test.nessie.server.authorization.rules.allow_updating_entity",
            "op in ['VIEW_REFERENCE', 'UPDATE_ENTITY'] "
                + "&& role=='test_user' && path.startsWith('allowed.') && ref.startsWith('allowedBranch'))")
        .put(
            "%test.nessie.server.authorization.rules.allow_deleting_entity",
            "op in ['VIEW_REFERENCE', 'DELETE_ENTITY'] "
                + "&& role=='test_user' && path.startsWith('allowed.') && ref.startsWith('allowedBranch'))")
        .put(
            "%test.nessie.server.authorization.rules.allow_commits_without_entity_changes",
            "op=='COMMIT_CHANGE_AGAINST_REFERENCE' && role=='test_user2' && ref.startsWith('allowedBranch')")
        .build();
  }
}
