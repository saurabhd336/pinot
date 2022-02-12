package org.apache.pinot.client;

import java.util.concurrent.Future;


public class PinotNettyClientTest {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("USAGE PinotHttpClientTest <ZK_URL> <pool_size> <query>");
      System.exit(1);
    }
    String zkUrl = args[0];
    Integer poolSize = Integer.parseInt(args[1]);

    PinotNettyClient.PinotNettyClientConfig pinotNettyClientConfig = new PinotNettyClient.PinotNettyClientConfig();
    pinotNettyClientConfig.setPerHostPoolSize(poolSize);
    pinotNettyClientConfig.setZkUrl(zkUrl);

    PinotNettyClient pinotHttpClient = new PinotNettyClient(pinotNettyClientConfig);

    try {
      Future<ResultSetGroup> resultSetGroupFuture = pinotHttpClient.execute(args[2]);
      ResultSetGroup resultSetGroup = resultSetGroupFuture.get();
      System.out.println(resultSetGroup);
    } catch (Exception exception) {
      // logs
    }
  }

}
