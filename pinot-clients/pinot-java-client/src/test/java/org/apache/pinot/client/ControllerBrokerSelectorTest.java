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
package org.apache.pinot.client;

import org.testng.annotations.Test;


public class ControllerBrokerSelectorTest {
  @Test
  public void testControllerConnection() {
//    BrokerSelector brokerSelector = new ControllerBrokerSelector("localhost")
  }

  public static void main(String[] args) {
    String scheme = "http";
    String controllerHost = "localhost";
    int controllerPort = 9000;

    Connection connection = ConnectionFactory.fromController(scheme, controllerHost, controllerPort);
    String tableName = "transcript";
    ResultSetGroup resultSetGroup = connection.execute(tableName, "select * from transcript");
    System.out.println(resultSetGroup);
    connection.close();
  }
}