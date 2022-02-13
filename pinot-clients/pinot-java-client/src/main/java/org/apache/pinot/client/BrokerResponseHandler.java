package org.apache.pinot.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.concurrent.Promise;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;


public class BrokerResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
  private ChannelHandlerContext cntx;
  private final BlockingQueue<Promise<ResultSetGroup>> messageList = new ArrayBlockingQueue<>(1024);
  private static final ObjectReader OBJECT_READER = new ObjectMapper().reader();

  @Override
  public void channelActive(ChannelHandlerContext ctx)
      throws Exception {
    super.channelActive(ctx);
    this.cntx = ctx;
  }

  public Future<ResultSetGroup> executeRequest(FullHttpRequest fullHttpRequest) {
    Promise<ResultSetGroup> promise = cntx.executor().newPromise();
    return executeRequest(fullHttpRequest, promise);
  }

  private Future<ResultSetGroup> executeRequest(FullHttpRequest fullHttpRequest, Promise<ResultSetGroup> promise) {
    messageList.add(promise);
    cntx.writeAndFlush(fullHttpRequest);
    return promise;
  }


  @Override
  public void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
    // TODO handle non 200 responses. Use promeise.setFailure()
    String responseBody = msg.content().toString(StandardCharsets.UTF_8);
    messageList.poll().setSuccess(new ResultSetGroup(BrokerResponse.fromJson(OBJECT_READER.readTree(responseBody))));
  }

  @Override
  public void exceptionCaught(
      ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.close();
  }


}
