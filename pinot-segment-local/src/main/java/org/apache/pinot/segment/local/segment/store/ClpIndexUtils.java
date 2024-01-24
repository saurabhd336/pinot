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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;

public class ClpIndexUtils {
  private ClpIndexUtils() {
  }
  public static boolean hasClpIndex(File segmentDir, String columnName) {
    return TextIndexUtils.hasTextIndex(segmentDir, columnName + "_logtype_clp") &&
        TextIndexUtils.hasTextIndex(segmentDir, columnName + "_dictvar_clp") &&
        TextIndexUtils.hasTextIndex(segmentDir, columnName + "_encodedvar_clp");
  }

  public static void cleanupClpIndex(File segmentDir, String columnName) {
    TextIndexUtils.cleanupTextIndex(segmentDir, columnName + "_logtype_clp");
    TextIndexUtils.cleanupTextIndex(segmentDir, columnName + "_dictvar_clp");
    TextIndexUtils.cleanupTextIndex(segmentDir, columnName + "_encodedvar_clp");
  }
}
