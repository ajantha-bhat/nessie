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
package org.projectnessie.gc.base;

import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;

/** A utility class wrapping bloom filter functionality. */
public class ContentBloomFilter implements Serializable {

  private static final long serialVersionUID = -693336833916979221L;

  // track iceberg table/view contents only using the snapshot/version id
  // as the consumer of GC results needs only this info for clean up.
  // String bloom filter with content type prefix + snapshot/version id.
  private final BloomFilter<String> filter;

  public ContentBloomFilter(long expectedEntries, double bloomFilterFpp) {
    this.filter =
        BloomFilter.create(
            Funnels.stringFunnel(StandardCharsets.UTF_8), expectedEntries, bloomFilterFpp);
  }

  public void put(Content content) {
    filter.put(getValue(content));
  }

  public boolean mightContain(Content content) {
    return filter.mightContain(getValue(content));
  }

  public void merge(ContentBloomFilter filter) {
    if (filter.filter != null) {
      this.filter.putAll(filter.filter);
    }
  }

  public double getExpectedFpp() {
    return filter.expectedFpp();
  }

  private String getValue(Content content) {
    // For the contents before global state removal (Nessie version < 0.26.0)
    // metadataLocation will not be unique.
    // Hence, to handle both the version's compatibility, use snapshotId + metadataLocation as key.
    switch (content.getType()) {
      case ICEBERG_TABLE:
        IcebergTable icebergTable = (IcebergTable) content;
        return icebergTable.getSnapshotId() + icebergTable.getMetadataLocation();
      case ICEBERG_VIEW:
        IcebergView icebergView = (IcebergView) content;
        return icebergView.getVersionId() + icebergView.getMetadataLocation();
      default:
        throw new RuntimeException("Unsupported type " + content.getType());
    }
  }
}
