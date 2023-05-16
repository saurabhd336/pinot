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

package org.apache.pinot.common.restlet.resources.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class SegmentReloadStatusValue {
  private final int _totalSegmentCount;
  private final int _successCount;
  private final String _instanceName;
  private final boolean _processing;

  private final String _throwable;

  @JsonCreator
  public SegmentReloadStatusValue(@JsonProperty(value = "totalSegmentCount", required = true) int totalSegmentCount,
      @JsonProperty(value = "successCount", required = true) int successCount,
      @JsonProperty(value = "instanceName", required = true) String instanceName,
      @JsonProperty(value = "processing", required = true) boolean processing,
      @JsonProperty(value = "exception", required = true) String exception) {
    _totalSegmentCount = totalSegmentCount;
    _successCount = successCount;
    _instanceName = instanceName;
    _processing = processing;
    _throwable = exception;
  }

  public int getTotalSegmentCount() {
    return _totalSegmentCount;
  }

  public int getSuccessCount() {
    return _successCount;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public boolean isProcessing() {
    return _processing;
  }

  public String getException() {
    return _throwable;
  }
}
