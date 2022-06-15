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
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.server.starter.helix.SegmentReloadTaskStatusCache;
import org.apache.pinot.spi.utils.JsonUtils;


@Api(tags = "Tasks")
@Path("/")
public class TaskStatusResource {
  @GET
  @Path("/task/status/{taskId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Task status", notes = "Return status of the given task")
  public String taskStatus(@PathParam("taskId") String taskId)
      throws Exception {
    SegmentReloadTaskStatusCache.SegmentReloadStatusValue segmentReloadStatusValue =
        SegmentReloadTaskStatusCache.getStatus(taskId);

    return JsonUtils.objectToString(segmentReloadStatusValue);
  }
}