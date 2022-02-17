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

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.LogResponse;

public final class GCUtil {

  private GCUtil() {}

  /**
   * Traverse the live commits stream till an entry is seen for each live content key.
   *
   * @param liveCommitPredicate predicate to identify the commit as live
   * @param isLiveContentsKeyAdded check point to enable the validation
   * @param liveContentKeys live content keys at the point of cutoff time
   * @param commits stream of {@link LogResponse.LogEntry}
   * @param commitHandler consumer of {@link LogResponse.LogEntry}
   */
  public static void traverseLiveCommits(
      Predicate<CommitMeta> liveCommitPredicate,
      MutableBoolean isLiveContentsKeyAdded,
      Set<ContentKey> liveContentKeys,
      Stream<LogResponse.LogEntry> commits,
      Consumer<LogResponse.LogEntry> commitHandler) {
    Spliterator<LogResponse.LogEntry> src = commits.spliterator();
    // Use a Spliterator to limit the processed commits to the "live" commits - i.e. stop traversing
    // the expired
    // commits once an entry is seen for each live content key.
    new Spliterators.AbstractSpliterator<LogResponse.LogEntry>(src.estimateSize(), 0) {
      private boolean more = true;

      @Override
      public boolean tryAdvance(Consumer<? super LogResponse.LogEntry> action) {
        if (!more) {
          return false;
        }
        more =
            src.tryAdvance(
                logEntry -> {
                  if (!liveCommitPredicate.test(logEntry.getCommitMeta())
                      && isLiveContentsKeyAdded.isTrue()
                      && (liveContentKeys.isEmpty())) {
                    // can stop traversing as we processed all the live commits
                    // and found the head commit of all the live content keys after cutoff time.
                    more = false;
                  } else {
                    // process this commit entry.
                    action.accept(logEntry);
                  }
                });
        return more;
      }
    }.forEachRemaining(commitHandler);
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
    NessieClientBuilder builder;
    if (clientBuilderClassName == null) {
      // Use the default HttpClientBuilder
      builder = HttpClientBuilder.builder();
    } else {
      // Use the custom client builder
      try {
        builder =
            (NessieClientBuilder)
                Class.forName(clientBuilderClassName).getDeclaredConstructor().newInstance();
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
    return (NessieApiV1) builder.fromConfig(configuration::get).build(NessieApiV1.class);
  }
}
