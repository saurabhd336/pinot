package org.apache.pinot.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;


public class PinotNettyClient {
  public static class PinotNettyClientConfig {
    private int perHostPoolSize;
    private String zkUrl;

    public int getPerHostPoolSize() {
      return perHostPoolSize;
    }

    public void setPerHostPoolSize(int perHostPoolSize) {
      this.perHostPoolSize = perHostPoolSize;
    }

    public String getZkUrl() {
      return zkUrl;
    }

    public void setZkUrl(String zkUrl) {
      this.zkUrl = zkUrl;
    }
  }

  private final PinotNettyClientConfig _config;
  private final BrokerSelector _brokerSelector;
  // brokerHostPort -> Pool of channels (connections?)
  private final ConcurrentHashMap<String, BrokerConnectionPool> _channelPoolMap;

  public PinotNettyClient(PinotNettyClientConfig config) {
    this._config = config;
    this._brokerSelector = new DynamicBrokerSelector(_config.zkUrl);
    this._channelPoolMap = new ConcurrentHashMap<>();
  }

  public Future<ResultSetGroup> execute(String query) throws Exception {
    String brokerHostPort = _brokerSelector.selectBroker(null);
    BrokerConnectionPool brokerConnectionPool1 =
        _channelPoolMap.computeIfAbsent(brokerHostPort, k -> {
          try {
            return BrokerConnectionPool.createNewPool(brokerHostPort, _config.perHostPoolSize);
          } catch (Exception e) {
            // logs
          }
          return null;
        });
    BrokerConnectionPool brokerConnectionPool = _channelPoolMap.get(brokerHostPort);
    return brokerConnectionPool.execute(query);
  }

}
