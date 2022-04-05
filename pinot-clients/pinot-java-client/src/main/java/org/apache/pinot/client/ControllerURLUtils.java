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

/**
 * Util class for Controller APIs
 */
public class ControllerURLUtils {
  private ControllerURLUtils() {
    //not called
  }
  // Other URLs to follow
  private static final String TABLE_TO_BROKER_MAP_URL_FORMAT = "%s://%s:%d/v2/brokers/tables";

  public static String getTableBrokerUrl(String scheme, String controllerHost, int controllerPort) {
    return String.format(TABLE_TO_BROKER_MAP_URL_FORMAT, scheme, controllerHost, controllerPort);
  }
}
