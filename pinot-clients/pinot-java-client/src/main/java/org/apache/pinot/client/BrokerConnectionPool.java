package org.apache.pinot.client;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class BrokerConnectionPool {
  private String _hostPort;
  private BrokerChannelPool _brokerChannelPool;
  public BrokerConnectionPool(String hostPort, int poolSizePerHost) throws Exception {
    this._hostPort = hostPort;
    this._brokerChannelPool = new BrokerChannelPool(_hostPort, poolSizePerHost);
    _brokerChannelPool.init();
  }

  public Future<ResultSetGroup> execute(String query) throws Exception {
    BrokerChannel brokerChannel = _brokerChannelPool.lease(1000, TimeUnit.MILLISECONDS);
    Future<ResultSetGroup> result = brokerChannel.executeQuery(query);
    _brokerChannelPool.release(brokerChannel);
    return result;
  }

  public static BrokerConnectionPool createNewPool(String hostPort, int poolSizePerHost) throws Exception {
    return new BrokerConnectionPool(hostPort, poolSizePerHost);
  }
}
