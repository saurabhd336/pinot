/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.server.starter.helix;

import com.google.common.cache.CacheBuilder;
import java.util.concurrent.ConcurrentMap;

public class SegmentMessageHandlerStatusManager {

  public enum SegmentMessageHandlerStatus {
    IN_PROGRESS,
    SUCCESS,
    FAILED
  }

  private static final ConcurrentMap<String, HandlerStatus> MESSAGE_ID_TO_STATUS_MAP =
      CacheBuilder.newBuilder()
          .maximumSize(1000L)
          .<String, HandlerStatus>build().asMap();

  public static class HandlerStatus {
    private volatile SegmentMessageHandlerStatus _status;
    private volatile String _exception;
    public HandlerStatus() {
      _status = SegmentMessageHandlerStatus.IN_PROGRESS;
    }

    public void markSuccess() {
      _status = SegmentMessageHandlerStatus.SUCCESS;
    }
    public void setException(Throwable e) {
      _status = SegmentMessageHandlerStatus.FAILED;
      _exception = e.toString();
    }
    public SegmentMessageHandlerStatus getStatus() {
      return _status;
    }
    public String getException() {
      return _exception;
    }
  }

  public static void init(String messageId) {
    MESSAGE_ID_TO_STATUS_MAP.put(messageId, new HandlerStatus());
  }

  public static void markSuccess(String messageId) {
    MESSAGE_ID_TO_STATUS_MAP.get(messageId).markSuccess();
  }

  public static void markFailed(String messageId, Throwable e) {
    MESSAGE_ID_TO_STATUS_MAP.get(messageId).setException(e);
  }

  public static HandlerStatus getStatus(String messageId) {
    return MESSAGE_ID_TO_STATUS_MAP.get(messageId);
  }
}
