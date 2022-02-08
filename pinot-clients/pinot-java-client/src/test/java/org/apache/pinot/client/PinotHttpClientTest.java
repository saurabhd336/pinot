package org.apache.pinot.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PinotHttpClientTest {
    public static void main(String[] args) throws InterruptedException {
        if (args.length != 3) {
            System.err.println("USAGE PinotHttpClientTest <ZK_URL> <pool_size> <query>");
            System.exit(1);
        }
        String zkUrl = args[0];
        Integer poolSize = Integer.parseInt(args[1]);

        PinotHttpClient.PinotHttpClientConfig pinotHttpClientConfig = new PinotHttpClient.PinotHttpClientConfig();
        pinotHttpClientConfig.setPoolSize(poolSize);
        pinotHttpClientConfig.setZkUrl(zkUrl);
        pinotHttpClientConfig.setConnectionTimeOutMillis(TimeUnit.SECONDS.toMillis(10));

        PinotHttpClient pinotHttpClient = new PinotHttpClient(pinotHttpClientConfig);
        pinotHttpClient.init();

        try {
            ResultSetGroup resultSetGroup = pinotHttpClient.execute(args[2]);
            System.out.println(resultSetGroup);
        } catch (TimeoutException timeoutException) {
            // logs
        } catch (InterruptedException interruptedException) {
            // logs
        }
    }
}
