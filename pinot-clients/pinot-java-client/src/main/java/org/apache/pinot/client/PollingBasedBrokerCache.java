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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;


/**
 * Maintains table -> list of brokers, supports update
 */
public class PollingBasedBrokerCache {

  private static class BrokerInstance {
    @JsonProperty("host")
    public String _host;

    @JsonProperty("port")
    public int _port;

    @JsonProperty("instanceName")
    public String _instanceName;
  }

  public static class BrokerData {
    public Map<String, List<String>> _tableToBrokerMap;
    public List<String> _brokers;

    public BrokerData(Map<String, List<String>> tableToBrokerMap, List<String> brokers) {
      _tableToBrokerMap = tableToBrokerMap;
      _brokers = brokers;
    }
  }

  private volatile BrokerData _brokerData;
  private final Random _random = new Random();
  private final AsyncHttpClient _client;
  private final String _address;
  private static final TypeReference<Map<String, List<BrokerInstance>>> RESPONSE_TYPE_REF =
      new TypeReference<Map<String, List<BrokerInstance>>>() { };
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public PollingBasedBrokerCache(String scheme, String controllerHost, int controllerPort) {
    _client = Dsl.asyncHttpClient();
    _address = ControllerURLUtils.getTableBrokerUrl(scheme, controllerHost, controllerPort);
  }

  protected void updateBrokerData() throws Exception {
    BoundRequestBuilder getRequest = _client.prepareGet(_address);
    Future<Response> responseFuture = getRequest.addHeader("accept", "application/json").execute();
    Response response = responseFuture.get();
    String responseBody = response.getResponseBody(StandardCharsets.UTF_8);
    Map<String, List<BrokerInstance>> responses = OBJECT_MAPPER.readValue(responseBody, RESPONSE_TYPE_REF);
    List<String> brokers = new ArrayList<>();
    Map<String, List<String>> tableToBrokersMap = new HashMap<>();
    for (Map.Entry<String, List<BrokerInstance>> tableToBrokers: responses.entrySet()) {
      List<String> brokersForTable = new ArrayList<>();
      tableToBrokers.getValue().forEach(br -> {
        brokersForTable.add(br._host + ":" + br._port);
      });
      tableToBrokersMap.put(tableToBrokers.getKey(), brokersForTable);
      brokers.addAll(brokersForTable);
    }

    _brokerData = new BrokerData(tableToBrokersMap, brokers);
  }

  public String getBroker(String tableName) {
    List<String> brokers = (tableName == null) ? _brokerData._brokers : _brokerData._tableToBrokerMap.get(tableName);
    return brokers.get(_random.nextInt(brokers.size()));
  }

  public List<String> getBrokers() {
    return _brokerData._brokers;
  }
}
