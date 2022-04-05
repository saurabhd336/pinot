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

import java.util.List;


/**
 * Controller based broker cache that only supports manual refresh
 */
public class ManuallyUpdatableBrokerCache implements UpdatableBrokerCache {
  private final PollingBasedBrokerCache _pollingBasedBrokerCache;

  public ManuallyUpdatableBrokerCache(String scheme, String controllerHost, int controllerPort) {
    _pollingBasedBrokerCache = new PollingBasedBrokerCache(scheme, controllerHost, controllerPort);
  }

  public void init() { }

  @Override
  public String getBroker(String tableName) {
    return _pollingBasedBrokerCache.getBroker(tableName);
  }

  @Override
  public List<String> getBrokers() {
    return _pollingBasedBrokerCache.getBrokers();
  }

  @Override
  public void triggerBrokerCacheUpdate() throws Exception {
    _pollingBasedBrokerCache.updateBrokerData();
  }

  @Override
  public void close() {
  }
}
