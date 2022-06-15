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
package org.apache.pinot.controller.api.resources;

import org.apache.pinot.common.metadata.task.TaskType;


public class ServerReloadTaskStatusResponse {
  private TaskType _taskType;
  private long _taskSubmissionTimeInMillisEpoch;
  private int _totalSegmentCount;
  private int _successCount;

  public TaskType getTaskType() {
    return _taskType;
  }

  public void setTaskType(TaskType taskType) {
    _taskType = taskType;
  }

  public long getTaskSubmissionTimeInMillisEpoch() {
    return _taskSubmissionTimeInMillisEpoch;
  }

  public void setTaskSubmissionTimeInMillisEpoch(long taskSubmissionTimeInMillisEpoch) {
    _taskSubmissionTimeInMillisEpoch = taskSubmissionTimeInMillisEpoch;
  }

  public int getTotalSegmentCount() {
    return _totalSegmentCount;
  }

  public void setTotalSegmentCount(int totalSegmentCount) {
    _totalSegmentCount = totalSegmentCount;
  }

  public int getSuccessCount() {
    return _successCount;
  }

  public void setSuccessCount(int successCount) {
    _successCount = successCount;
  }
}