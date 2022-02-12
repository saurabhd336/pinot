package org.apache.pinot.client;

public class BrokerChannelPool extends ObjectPool<BrokerChannel> {

  private final String _host;
  private final int _port;

  public BrokerChannelPool(String hostPort, int poolSize) {
    super(poolSize);

    String[] hostAndPost = hostPort.split(":");

    this._host = hostAndPost[0];
    this._port = Integer.parseInt(hostAndPost[1]);
  }

  @Override
  BrokerChannel createObject() throws Exception {
    return new BrokerChannel(_host, _port);
  }

  @Override
  boolean validate(BrokerChannel obj) {
    // We could check if channel is closed?
    return true;
  }
}
