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
package org.apache.pinot.segment.local.segment.index;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.clp.ClpIndexCreatorV1;
import org.apache.pinot.segment.local.segment.index.readers.clp.ClpIndexReaderV1;
import org.apache.pinot.segment.spi.index.creator.ClpIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ClpIndexReader;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;


public class CLPIndexCreatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "CLPIndexCreatorTest");

  @Test
  public void testCLPIndex()
      throws Exception {
    List<String> logLines = new ArrayList<>();
    logLines.add(
        "2023/10/26 00:03:10.168 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32c_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32c_DEFAULT : Refreshed 35 property LiveInstance took 5 ms. Selective: true");
    logLines.add(
        "2023/10/26 00:03:10.169 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32d_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32d_DEFAULT : Refreshed 81 property LiveInstance took 4 ms. Selective: true");
    logLines.add(
        "2023/10/27 16:35:10.470 INFO [ControllerResponseFilter] [grizzly-http-server-2] Handled request from 10.12"
            + ".15.1 GET https://10.12.15.10:8443/health?checkType=liveness, content-type null status code 200 OK");
    logLines.add(
        "2023/10/27 16:35:10.607 INFO [ControllerResponseFilter] [grizzly-http-server-6] Handled request from 10.12"
            + ".19.5 GET https://pinot-pinot-broker-headless.managed.svc.cluster.local:8093/tables, content-type "
            + "application/json status code 200 OK");

    ClpIndexCreator clpIndexCreator = new ClpIndexCreatorV1(TEMP_DIR, "clpColumnName");
    for (int i = 0; i < logLines.size(); i++) {
      clpIndexCreator.add(logLines.get(i), -1);
    }

    clpIndexCreator.seal();
    clpIndexCreator.close();

    ClpIndexReader clpIndexReader = new ClpIndexReaderV1(TEMP_DIR, logLines.size(), "clpColumnName");
    Assert.assertEquals(clpIndexReader.getDocIds("*freshed 35 property LiveInstance took 5*").getCardinality(), 1);
    System.out.println(
        logLines.get(clpIndexReader.getDocIds("*freshed 35 property LiveInstance took 5 ms*").iterator().next()));

    clpIndexReader.getDocIds("*Handled request from 10.12.15*").forEach((int docId) -> {
      System.out.println(logLines.get(docId));
    });
  }

  @AfterTest
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
