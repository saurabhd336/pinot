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
package org.apache.pinot.segment.local.segment.index.clp;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.clp.ClpIndexCreatorV1;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.ClpIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.clp.ClpIndexReaderV1;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ClpIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ClpIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ClpIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;

public class ClpIndexType extends AbstractIndexType<ClpIndexConfig, ClpIndexReader, ClpIndexCreator> {
  public static final ClpIndexType INSTANCE = new ClpIndexType();
  private static final String DISPLAY_NAME = "clp";

  public ClpIndexType() {
    super(StandardIndexes.CLP_ID);
  }

  @Override
  public String getPrettyName() {
    return DISPLAY_NAME;
  }

  @Override
  public Class<ClpIndexConfig> getIndexConfigClass() {
    return ClpIndexConfig.class;
  }

  @Override
  public ClpIndexConfig getDefaultConfig() {
    return ClpIndexConfig.DISABLED;
  }

  @Override
  protected ColumnConfigDeserializer<ClpIndexConfig> createDeserializer() {
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass());
  }

  @Override
  public ClpIndexCreator createIndexCreator(IndexCreationContext context, ClpIndexConfig indexConfig)
      throws IOException {
    return new ClpIndexCreatorV1(context.getIndexDir(), context.getFieldSpec().getName());
  }

  @Override
  protected IndexReaderFactory<ClpIndexReader> createReaderFactory() {
    return new IndexReaderFactory<ClpIndexReader>() {
      @Nullable
      @Override
      public ClpIndexReader createIndexReader(SegmentDirectory.Reader segmentReader,
          FieldIndexConfigs fieldIndexConfigs, ColumnMetadata metadata)
          throws IOException {
        return new ClpIndexReaderV1(segmentReader.toSegmentDirectory().getPath().toFile(), metadata.getTotalDocs(),
            metadata.getColumnName());
      }
    };
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return null;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    Map<String, ClpIndexConfig> clpIndexConfigMap = FieldIndexConfigsUtil.enableConfigByColumn(INSTANCE, configsByCol);
    return new ClpIndexHandler(segmentDirectory.getSegmentMetadata(), clpIndexConfigMap);
  }
}
