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

package org.apache.pinot.common.messages;

import java.util.UUID;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;

public class HelixTaskGeneratorDebugMessage extends Message {
  public static final String GET_GENERATOR_DEBUG_LOGS_SUBTYPE = "GET_GENERATOR_DEBUG_LOGS";
  public static final String TABLE_NAME_WITH_TYPE_KEY = "TABLE_NAME_WITH_TYPE";
  public static final String TASK_TYPE_KEY = "TASK_TYPE";
  public static final String TASK_GENERATOR_DEBUG_DATA_KEY = "TASK_GENERATOR_DEBUG_DATA";

  public HelixTaskGeneratorDebugMessage(String tableNameWithType, String taskType) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(GET_GENERATOR_DEBUG_LOGS_SUBTYPE);
    setExecutionTimeout(-1);
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME_WITH_TYPE_KEY, tableNameWithType);
    znRecord.setSimpleField(TASK_TYPE_KEY, taskType);
  }

  public HelixTaskGeneratorDebugMessage(Message message) {
    super(message.getRecord());
  }

  public String getTableNameWithType() {
    return getRecord().getSimpleField(TABLE_NAME_WITH_TYPE_KEY);
  }

  public String getTaskType() {
    return getRecord().getSimpleField(TASK_TYPE_KEY);
  }
}
