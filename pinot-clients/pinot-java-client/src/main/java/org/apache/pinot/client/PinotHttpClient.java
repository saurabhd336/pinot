package org.apache.pinot.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PinotHttpClient {

    public static class PinotHttpClientConfig {
        private int poolSize;
        private String zkUrl;
        private long connectionTimeOutMillis;

        public int getPoolSize() {
            return poolSize;
        }

        public void setPoolSize(int poolSize) {
            this.poolSize = poolSize;
        }

        public String getZkUrl() {
            return zkUrl;
        }

        public void setZkUrl(String zkUrl) {
            this.zkUrl = zkUrl;
        }

        public long getConnectionTimeOutMillis() {
            return connectionTimeOutMillis;
        }

        public void setConnectionTimeOutMillis(long connectionTimeOutMillis) {
            this.connectionTimeOutMillis = connectionTimeOutMillis;
        }
    }
    private PinotHttpClientConfig _pinotHttpClientConfig;
    private ConnectionPool _connectionPool;


    public PinotHttpClient(PinotHttpClientConfig config) {
        this._pinotHttpClientConfig = config;
        this._connectionPool = new ConnectionPool(config.poolSize, config.zkUrl);
    }

    public void init() throws InterruptedException {
        _connectionPool.init();
    }

    public ResultSetGroup execute(String query) throws TimeoutException, InterruptedException {
        Connection connection = _connectionPool.lease(_pinotHttpClientConfig.connectionTimeOutMillis, TimeUnit.MILLISECONDS);
        Request request = new Request("sql", query);
        ResultSetGroup result = connection.execute(request);
        _connectionPool.release(connection);
        return result;
    }

}
