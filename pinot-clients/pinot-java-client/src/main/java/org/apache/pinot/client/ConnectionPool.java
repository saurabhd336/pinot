package org.apache.pinot.client;

public class ConnectionPool extends ObjectPool<Connection> {
    private final String zkUrl;

    public ConnectionPool(int maxConnections, String zkUrl) {
        super(maxConnections);
        this.zkUrl = zkUrl;
    }

    @Override
    Connection createObject() {
        return ConnectionFactory.fromZookeeper(zkUrl);
    }

    @Override
    boolean validate(Connection obj) {
        // We could close this connection if certain validations fail (ttl expiry?) and return false
        return true;
    }
}
