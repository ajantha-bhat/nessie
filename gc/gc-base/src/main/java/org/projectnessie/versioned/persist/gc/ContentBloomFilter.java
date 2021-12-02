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
package org.projectnessie.versioned.persist.gc;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.projectnessie.model.Content;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;

/** A utility class wrapping bloom filter functionality. */
public class ContentBloomFilter implements Serializable {

  private long expectedEntries;
  private double fpp;
  private static final long serialVersionUID = -693336833916979221L;

  // track iceberg contents only using the snapshot id (long bloom filter)
  // as the consumer of GC results needs only this info for clean up.
  private volatile BloomFilter<Long> icebergContentFilter;
  // TODO: understand delta lake content object and handle accordingly
  // track delta lake contents only using the checkpoints (string bloom filter)
  // as the consumer of GC results needs only this info for clean up.
  private volatile BloomFilter<String> deltaLakeContentFilter;
  // track view contents only using the version-id (int bloom filter)
  // as the consumer of GC results needs only this info for clean up.
  private volatile BloomFilter<Integer> viewContentFilter;

  public ContentBloomFilter(Map<String, String> config) {
    this.expectedEntries =
        Long.parseLong(
            config.getOrDefault(
                "nessie.gc.expected.per.table.bloomfilter.size", String.valueOf(1000000)));
    this.fpp =
        Double.parseDouble(
            config.getOrDefault(
                "nessie.gc.allowed.fpp.per.table.bloomfilter", String.valueOf(0.03D)));
  }

  private BloomFilter<Long> getIcebergContentFilter() {
    if (icebergContentFilter == null) {
      synchronized (this) {
        if (icebergContentFilter == null) {
          icebergContentFilter = BloomFilter.create(Funnels.longFunnel(), expectedEntries, fpp);
        }
      }
    }
    return icebergContentFilter;
  }

  private BloomFilter<String> getDeltaLakeContentFilter() {
    if (deltaLakeContentFilter == null) {
      synchronized (this) {
        if (deltaLakeContentFilter == null) {
          deltaLakeContentFilter =
              BloomFilter.create(
                  Funnels.stringFunnel(StandardCharsets.UTF_8), expectedEntries, fpp);
        }
      }
    }
    return deltaLakeContentFilter;
  }

  private BloomFilter<Integer> getViewContentFilter() {
    if (viewContentFilter == null) {
      synchronized (this) {
        if (viewContentFilter == null) {
          viewContentFilter = BloomFilter.create(Funnels.integerFunnel(), expectedEntries, fpp);
        }
      }
    }
    return viewContentFilter;
  }

  public boolean put(Content content) {
    switch (content.getType()) {
      case ICEBERG_TABLE:
        return getIcebergContentFilter().put(((IcebergTable) content).getSnapshotId());
      case DELTA_LAKE_TABLE:
        // TODO: understand delta lake content object and handle accordingly
        return getDeltaLakeContentFilter().put(((DeltaLakeTable) content).getLastCheckpoint());
      case ICEBERG_VIEW:
        return getViewContentFilter().put(((IcebergView) content).getVersionId());
      default:
        throw new RuntimeException("Unsupported type" + content.getType());
    }
  }

  public boolean mightContain(Content content) {
    switch (content.getType()) {
      case ICEBERG_TABLE:
        return getIcebergContentFilter().mightContain(((IcebergTable) content).getSnapshotId());
      case DELTA_LAKE_TABLE:
        // TODO: understand delta lake content object and handle accordingly
        return getDeltaLakeContentFilter()
            .mightContain(((DeltaLakeTable) content).getLastCheckpoint());
      case ICEBERG_VIEW:
        return getViewContentFilter().mightContain(((IcebergView) content).getVersionId());
      default:
        throw new RuntimeException("Unsupported type" + content.getType());
    }
  }

  public ContentBloomFilter merge(ContentBloomFilter filter) {
    if (filter.icebergContentFilter != null) {
      getIcebergContentFilter().putAll(icebergContentFilter);
    }
    if (filter.deltaLakeContentFilter != null) {
      getDeltaLakeContentFilter().putAll(deltaLakeContentFilter);
    }
    if (filter.viewContentFilter != null) {
      getViewContentFilter().putAll(viewContentFilter);
    }
    return this;
  }
}
