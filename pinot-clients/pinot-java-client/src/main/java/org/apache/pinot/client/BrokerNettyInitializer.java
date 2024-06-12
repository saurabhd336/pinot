package org.apache.pinot.client;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseDecoder;


public class BrokerNettyInitializer extends ChannelInitializer<SocketChannel> {
  public BrokerNettyInitializer() {
  }

  @Override
  public void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline p = ch.pipeline();

//    p.addLast("log", new LoggingHandler(LogLevel.INFO));
    p.addLast("codec", new HttpClientCodec());
    p.addLast(new HttpContentDecompressor());
    p.addLast(new HttpResponseDecoder());
    p.addLast(new HttpObjectAggregator(1048576));
    p.addLast("handler", new BrokerResponseHandler());
  }
}
