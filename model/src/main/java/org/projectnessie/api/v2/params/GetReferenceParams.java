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
package org.projectnessie.api.v2.params;

import static org.projectnessie.api.v2.doc.ApiDoc.REF_GET_PARAMETER_DESCRIPTION;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.immutables.builder.Builder.Constructor;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.Validation;

public class GetReferenceParams {

  @Parameter(
      description = REF_GET_PARAMETER_DESCRIPTION,
      examples = {@ExampleObject(ref = "ref"), @ExampleObject(ref = "refDefault")})
  @PathParam("ref")
  @NotNull
  @Pattern(regexp = Validation.REF_NAME_PATH_REGEX, message = Validation.REF_NAME_MESSAGE)
  private String ref;

  @Parameter(
      description =
          "Specify how much information to be returned. Will fetch additional metadata for references if set to 'ALL'.\n\n"
              + "A returned Branch instance will have the following information:\n\n"
              + "- numCommitsAhead (number of commits ahead of the default branch)\n\n"
              + "- numCommitsBehind (number of commits behind the default branch)\n\n"
              + "- commitMetaOfHEAD (the commit metadata of the HEAD commit)\n\n"
              + "- commonAncestorHash (the hash of the common ancestor in relation to the default branch).\n\n"
              + "- numTotalCommits (the total number of commits in this reference).\n\n"
              + "A returned Tag instance will only contain the 'commitMetaOfHEAD' and 'numTotalCommits' fields.\n\n"
              + "Note that computing & fetching additional metadata might be computationally expensive on the server-side, so this flag should be used with care.")
  @QueryParam("fetch")
  @Nullable
  private FetchOption fetchOption;

  public GetReferenceParams() {}

  @Constructor
  GetReferenceParams(@NotNull String ref, @Nullable FetchOption fetchOption) {
    this.ref = ref;
    this.fetchOption = fetchOption;
  }

  @Nullable
  public FetchOption fetchOption() {
    return fetchOption;
  }

  public String getRef() {
    return ref;
  }

  public static GetReferenceParamsBuilder builder() {
    return new GetReferenceParamsBuilder();
  }
}
