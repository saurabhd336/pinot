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
package org.apache.pinot.tools.segment.reader;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


public class SegmentAnonymizerTest {

  public static void main(String[] args)
      throws Exception {
    String segmentPath = args[0];
    String tableConfigPath = args[1];
    String schemaPath = args[2];
    String outputPath = args[3];

    TableConfig tableConfig = JsonUtils.fileToObject(new File(tableConfigPath), TableConfig.class);
    Schema schema = JsonUtils.fileToObject(new File(schemaPath), Schema.class);

    // Create recordReader
    PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
    recordReader.init(new File(segmentPath), null, null, false,
        List.of("AirlineID", "ArrDel15", "CancellationCode"));

    // Create new segment with anonymized data
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(
        JsonUtils.fileToObject(new File(tableConfigPath), TableConfig.class),
        JsonUtils.fileToObject(new File(schemaPath), Schema.class));
    config.setOutDir(new File(outputPath).getPath());
    config.setTableName(tableConfig.getTableName());
    config.setSegmentName("anonymized_segment_" + UUID.randomUUID());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();
  }
}
