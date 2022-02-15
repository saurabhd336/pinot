package org.apache.pinot.client;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;


public class BrokerChannel {
  private String _host;
  private int _port;
  private BrokerResponseHandler _brokerResponseHandler;
  private Channel _channel;

  public BrokerChannel(String host, int port)
      throws Exception {
    EventLoopGroup group = new NioEventLoopGroup();
    Bootstrap b = new Bootstrap();
    b.group(group)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .channel(NioSocketChannel.class)
        .remoteAddress(new InetSocketAddress(host, port))
        .handler(new BrokerNettyInitializer());
    this._channel = b.connect().sync().channel();
    this._host = host;
    this._port = port;
    this._brokerResponseHandler = (BrokerResponseHandler) _channel.pipeline().last();
  }

  public Future<ResultSetGroup> executeQuery(String query) {
    FullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.POST, "/sql");

    ObjectNode json = JsonNodeFactory.instance.objectNode();
    json.put("sql", query);
    json.put("queryOptions", "groupByMode=sql;responseFormat=sql");


    request.headers().set(HttpHeaderNames.HOST, _host);
    request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE); // or HttpHeaders.Values.CLOSE
    request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);
    request.headers().set(HttpHeaderNames.ACCEPT, "application/json");
    request.headers().add(HttpHeaderNames.CONTENT_TYPE, "application/json");
    ByteBuf bbuf = Unpooled.copiedBuffer(json.toString(), StandardCharsets.UTF_8);
    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, bbuf.readableBytes());
    request.content().clear().writeBytes(bbuf);

    return _brokerResponseHandler.executeRequest(request);
  }
}
