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
package org.apache.pinot.integration.tests.tpch;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.integration.tests.tpch.generator.PinotQueryBasedColumnDataProvider;
import org.apache.pinot.integration.tests.tpch.generator.SampleColumnDataProvider;
import org.apache.pinot.integration.tests.tpch.generator.TPCHQueryGeneratorV2;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tools.utils.JarUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

// TODO: extract common functions from TPCHQueryIntegrationTest and SSBQueryIntegrationTest

/**
 * Integration test that tests Pinot using TPCH data.
 * Data is loaded into Pinot and H2 from /resources/examples/batch/tpch. The dataset size is very small, please follow
 * REAME.md to generate a larger dataset for better testing.
 * Queries are executed against Pinot and H2, and the results are compared.
 */
public class TPCHGeneratedQueryIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_TPCH_QUERIES = 10000;
  private static TPCHQueryGeneratorV2 _tpchQueryGenerator;
  private static final Boolean useMultiValue = true;

  // Pinot query 6 fails due to mismatch results.
  // Pinot queries 15, 16, 17 fail due to lack of support for views.
  // Pinot queries 23, 24 fail due to java heap space problem or timeout.
  private static final Set<Integer> EXEMPT_QUERIES = ImmutableSet.of(6, 15, 16, 17, 23, 24);

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    setUpH2Connection();
    for (String tableName : Constants.TPCH_TABLE_NAMES) {
      File tableSegmentDir = new File(_segmentDir, tableName);
      File tarDir = new File(_tarDir, tableName);
      String tableResourceFolder = Constants.getTableResourceFolder(tableName, useMultiValue);
      URL resourceUrl = getClass().getClassLoader().getResource(tableResourceFolder);
      Assert.assertNotNull(resourceUrl, "Unable to find resource from: " + tableResourceFolder);
      File resourceFile;
      if ("jar".equals(resourceUrl.getProtocol())) {
        String[] splits = resourceUrl.getFile().split("!");
        File tempUnpackDir = new File(_tempDir.getAbsolutePath() + File.separator + splits[1]);
        TestUtils.ensureDirectoriesExistAndEmpty(tempUnpackDir);
        JarUtils.copyResourcesToDirectory(splits[0], splits[1].substring(1), tempUnpackDir.getAbsolutePath());
        resourceFile = tempUnpackDir;
      } else {
        resourceFile = new File(resourceUrl.getFile());
      }
      File dataFile =
          new File(getClass().getClassLoader().getResource(Constants.getTableAvroFilePath(tableName, true)).getFile());
      Assert.assertTrue(dataFile.exists(), "Unable to load resource file from URL: " + dataFile);
      File schemaFile = new File(resourceFile.getPath(), tableName + "_schema.json");
      File tableFile = new File(resourceFile.getPath(), tableName + "_offline_table_config.json");
      // Pinot
      TestUtils.ensureDirectoriesExistAndEmpty(tableSegmentDir, tarDir);
      Schema schema = createSchema(schemaFile);
      addSchema(schema);
      TableConfig tableConfig = createTableConfig(tableFile);
      addTableConfig(tableConfig);
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(Collections.singletonList(dataFile), tableConfig, schema, 0,
          tableSegmentDir, tarDir);
      uploadSegments(tableName, tarDir);
      // H2
      ClusterIntegrationTestUtils.setUpH2TableWithAvro(Collections.singletonList(dataFile), tableName, _h2Connection);
    }

    SampleColumnDataProvider sampleColumnDataProvider =
        new PinotQueryBasedColumnDataProvider(new PinotQueryBasedColumnDataProvider.PinotConnectionProvider() {
          @Override
          public Connection getConnection() {
            return getPinotConnection();
          }
        });
    _tpchQueryGenerator = new TPCHQueryGeneratorV2(sampleColumnDataProvider);
    _tpchQueryGenerator.init();
    File file = new File("/Users/saurabh.dubey/Documents/workspace/stuff/tpch/results/tpch_results.csv");
    //Instantiating the PrintStream class
    PrintStream stream = new PrintStream(file);
    System.setErr(stream);
  }

  @Test(dataProvider = "QueryDataProvider")
  public void testTPCHQueries(String[] pinotAndH2Queries)
      throws Exception {
    String status = "SUCCESS";
    String failureReason = "NA";
    long numRows = -1;
    try {
      numRows = testQueriesSucceed(pinotAndH2Queries[1], pinotAndH2Queries[0]);
    } catch (Throwable t) {
      status = "FAILED";
      failureReason = t.getMessage();
    } finally {
      System.err.printf("\"%s\",\"%s\",\"%s\", %d\n", pinotAndH2Queries[1], status,
          StringEscapeUtils.escapeJavaScript(failureReason).replace(',', ' '), numRows);
    }
  }

  protected long testQueriesSucceed(String pinotQuery, String h2Query)
      throws Exception {
    ResultSetGroup pinotResultSetGroup = getPinotConnection().execute(pinotQuery);
    org.apache.pinot.client.ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);
    if (CollectionUtils.isNotEmpty(pinotResultSetGroup.getExceptions())) {
      Assert.fail(pinotResultSetGroup.getExceptions().get(0).toString());
    }

    int numRows = resultTableResultSet.getRowCount();
    int numColumns = resultTableResultSet.getColumnCount();

    // h2 response
    Assert.assertNotNull(_h2Connection);
    Statement h2statement = _h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    h2statement.setQueryTimeout(10);

    try {
      h2statement.execute(h2Query);
    } catch (SQLTimeoutException e) {
      Assert.fail("H2 query timed out!");
    }
    ResultSet h2ResultSet = h2statement.getResultSet();

    // compare results.
    Assert.assertEquals(numColumns, h2ResultSet.getMetaData().getColumnCount());
    if (h2ResultSet.first()) {
      for (int i = 0; i < numRows; i++) {
        for (int c = 0; c < numColumns; c++) {
          String h2Value = h2ResultSet.getString(c + 1);
          String pinotValue = resultTableResultSet.getString(i, c);
          boolean error = ClusterIntegrationTestUtils.fuzzyCompare(h2Value, pinotValue, pinotValue);
          if (error) {
            Assert.fail(
                String.format("Value: %d does not match at (%d, %d), expected h2 value: %s actual Pinot value: %s", c,
                    i, c, h2Value, pinotValue));
          }
        }
        if (!h2ResultSet.next() && i != numRows - 1) {
          Assert.fail(String.format("H2 result set is smaller than Pinot result set after: %d rows", i));
        }
      }
    }
    Assert.assertFalse(h2ResultSet.next(),
        String.format("Pinot result set is smaller than H2 result set after: %d rows!", numRows));

    return numRows;
  }

  @Override
  protected long getCurrentCountStarResult() {
    return getPinotConnection().execute("SELECT COUNT(*) FROM orders").getResultSet(0).getLong(0);
  }

  @Override
  protected long getCountStarResult() {
    return 9999L;
  }

  @Override
  protected boolean useMultiStageQueryEngine() {
    return true;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // unload all TPCH tables.
    for (String table : Constants.TPCH_TABLE_NAMES) {
      dropOfflineTable(table);
    }

    // stop components and clean up
    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }

  @DataProvider(name = "QueryDataProvider")
  public static Object[][] queryDataProvider()
      throws IOException {
    Object[][] queries = new Object[NUM_TPCH_QUERIES][];
    for (int i = 0; i < NUM_TPCH_QUERIES; i++) {
      queries[i] = new Object[2];
      Pair<String, String> query = _tpchQueryGenerator.generateRandomQuery();
      queries[i][0] = query.getLeft();
      queries[i][1] = query.getRight();
    }

    return queries;
  }
}
