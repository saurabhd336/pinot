/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.segment.index.clp.ClpIndexType;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ClpIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ClpIndexCreator;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClpIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClpIndexHandler.class);
  private final SegmentMetadata _segmentMetadata;
  private final Map<String, ClpIndexConfig> _clpIndexConfigs;

  public ClpIndexHandler(SegmentMetadataImpl segmentMetadata, Map<String, ClpIndexConfig> clpIndexConfigs) {
    _segmentMetadata = segmentMetadata;
    _clpIndexConfigs = clpIndexConfigs;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    removeIndexes(segmentWriter);
    buildIndexes(segmentWriter);
  }

  private void removeIndexes(SegmentDirectory.Writer segmentWriter) {
    // Remove indices not set in table config any more
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(ClpIndexType.INSTANCE);
    for (String column : existingColumns) {
      if (!_clpIndexConfigs.containsKey(column)) {
        LOGGER.info("Removing existing clp index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, ClpIndexType.INSTANCE);
        LOGGER.info("Removed existing clp index from segment: {}, column: {}", segmentName, column);
      }
    }
  }

  private void buildIndexes(SegmentDirectory.Writer segmentWriter) {
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(ClpIndexType.INSTANCE);
    for (String column : _clpIndexConfigs.keySet()) {
      if (!existingColumns.contains(column)) {
        LOGGER.info("Building clp index for segment: {}, column: {}", segmentName, column);
        try {
          buildClpIndex(segmentWriter, _segmentMetadata.getColumnMetadataFor(column), _clpIndexConfigs.get(column));
        } catch (Exception e) {
          LOGGER.error("Failed to build clp index for segment: {}, column: {}", segmentName, column, e);
          throw new RuntimeException(e);
        }
        LOGGER.info("Built clp index for segment: {}, column: {}", segmentName, column);
      }
    }
  }

  private void buildClpIndex(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      ClpIndexConfig indexConfig)
      throws Exception {
    IndexCreationContext context =
        IndexCreationContext.builder().withIndexDir(_segmentMetadata.getIndexDir()).withColumnMetadata(columnMetadata)
            .build();

    try (ClpIndexCreator clpIndexCreator = StandardIndexes.clp().createIndexCreator(context, indexConfig);
        ForwardIndexReader forwardIndexReader = ForwardIndexType.read(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext()) {
      for (int docId = 0; docId < columnMetadata.getTotalDocs(); docId++) {
        clpIndexCreator.add(forwardIndexReader.getString(docId, readerContext), -1);
      }
      clpIndexCreator.seal();
    }
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader)
      throws Exception {
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(ClpIndexType.INSTANCE);
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!_clpIndexConfigs.containsKey(column)) {
        LOGGER.info("Need to remove existing clp index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : _clpIndexConfigs.keySet()) {
      if (!existingColumns.contains(column)) {
        LOGGER.info("Need to create new clp index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter)
      throws Exception {
  }
}
