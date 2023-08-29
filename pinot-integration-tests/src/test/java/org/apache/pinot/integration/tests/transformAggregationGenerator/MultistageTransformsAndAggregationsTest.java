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
package org.apache.pinot.integration.tests.transformAggregationGenerator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MultistageTransformsAndAggregationsTest extends BaseClusterIntegrationTest {
  private static String MULTISTAGE_QUERY_TEMPLATE = "SET \"useMultistageEngine\"=true;\n"
      + "select col from (select %s as col, \"t1\".\"$docId\" as docId from transformsAndAggregationsTest t1 join "
      + "transformsAndAggregationsTest t2 "
      + "on \"t1\".\"$docId\" = \"t2\".\"$docId\") order by docId limit 100";
  private static String V1_QUERY_TEMPLATE =
      "select %s as col from transformsAndAggregationsTest order by $docId limit 100";

  private static final int NUM_QUERIES = 1000;
  private static Schema _schema;

  private static final Random RANDOM = new Random();

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    _schema = createSchema();
    addSchema(_schema);

    InputStream inputStream = BaseClusterIntegrationTest.class.getClassLoader().getResourceAsStream(getTableFileName());
    TableConfig offlineTableConfig = JsonUtils.inputStreamToObject(inputStream, TableConfig.class);
    addTableConfig(offlineTableConfig);

    File avroFile = new File(
        BaseClusterIntegrationTest.class.getClassLoader().getResource("transformsAggregationsTestData.avro").getPath());
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(Collections.singletonList(avroFile), offlineTableConfig, _schema,
        0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);
    waitForAllDocsLoaded(60000);

    Properties properties = getPinotConnectionProperties();
    _pinotConnection = ConnectionFactory.fromZookeeper(properties, getZkUrl() + "/" + getHelixClusterName());
    properties.put("useMultiStageEngine", "true");
    _pinotConnectionV2 = ConnectionFactory.fromZookeeper(properties, getZkUrl() + "/" + getHelixClusterName());
  }

  @Override
  protected String getSchemaFileName() {
    return "transformsAndAggregationsTestSchema.json";
  }

  @Override
  protected String getSchemaName() {
    return "transformsAndAggregationsTest";
  }

  @Override
  protected String getTableName() {
    return "transformsAndAggregationsTest";
  }

  protected long getCountStarResult() {
    return 10000;
  }

  protected String getTableFileName() {
    return "transformsAndAggregationsTestTableConfig.json";
  }

  @Test(dataProvider = "QueryDataProvider")
  public void test(String[] queries) {
    ResultSetGroup rg1 = _pinotConnection.execute(queries[0]);
    ResultSetGroup rg2 = _pinotConnectionV2.execute(queries[1]);

    if (rg2.getExceptions().size() > 0) {
      Assert.fail(rg2.getExceptions().get(0).getMessage());
    }

    ResultSet r1 = rg1.getResultSet(0);
    ResultSet r2 = rg2.getResultSet(0);

    // Compare and assert
    Assert.assertEquals(r1.getColumnCount(), r2.getColumnCount());
    Assert.assertEquals(r1.getRowCount(), r2.getRowCount());

    for (int i = 0; i < r1.getRowCount(); i++) {
      for (int c = 0; c < r1.getColumnCount(); c++) {
        String v1Value = r1.getString(i, c);
        String v2Value = r2.getString(i, c);
        boolean error = ClusterIntegrationTestUtils.fuzzyCompare(v1Value, v2Value, v2Value);
        if (error) {
          Assert.fail(
              String.format("Value: %d does not match at (%d, %d), expected v2 value: %s actual v2 value: %s", c, i, c,
                  v1Value, v2Value));
        }
      }
    }
  }

  @DataProvider(name = "QueryDataProvider")
  public static Object[][] queryDataProvider()
      throws IOException {
    Object[][] queries = new Object[NUM_QUERIES][];
    for (int i = 0; i < NUM_QUERIES; i++) {
      Pair<String, String> transform = getRandomTransform();
      queries[i] = new String[2];
      queries[i][0] = String.format(V1_QUERY_TEMPLATE, transform.getLeft());
      queries[i][1] = String.format(MULTISTAGE_QUERY_TEMPLATE, transform.getRight());
    }

    return queries;
  }

  private static Pair<String, String> getRandomTransform() {
    FieldSpec col = new ArrayList<>(_schema.getAllFieldSpecs()).get(RANDOM.nextInt(_schema.getAllFieldSpecs().size()));

    if (col.isSingleValueField()) {
      return AggregationAndTransformGenerator.generateTransformForSvColumn("t1", col);
    } else {
      return AggregationAndTransformGenerator.generateTransformForMvColumn("t1", col);
    }
  }
}
