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
package org.apache.pinot.integration.tests.transformAggregationGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.data.FieldSpec;


public class AggregationAndTransformGenerator {
  private static final Random RANDOM = new Random();
  private static final Map<FieldSpec.DataType, List<TransformFunction>> SV_TRANSFORMS = new HashMap<>();
  private static final Map<FieldSpec.DataType, List<TransformFunction>> MV_TRANSFORMS = new HashMap<>();
  private static final Map<FieldSpec.DataType, List<AggregationFunction>> AGGREGATIONS = new HashMap<>();

  static {
    populateAggregations();
    populateTransforms();
  }

  private static void populateAggregations() {

  }

  private static void populateTransforms() {
    Transforms.FUNCTIONS.forEach(t -> {
      t.getSupportedInputDataTypes().forEach(d -> {
        if (t.isSvSupported()) {
          SV_TRANSFORMS.computeIfAbsent(d, k -> new ArrayList<>()).add(t);
        }

        if (t.isMvSupported()) {
          MV_TRANSFORMS.computeIfAbsent(d, k -> new ArrayList<>()).add(t);
        }
      });
    });
  }

  public static Pair<String, String> generateTransformForSvColumn(String table, FieldSpec col) {
    FieldSpec.DataType dataType = col.getDataType();
    if (!SV_TRANSFORMS.containsKey(dataType)) {
      return Pair.of(col.getName(), "\"" + table + "\".\"" + col.getName() + "\"");
    }
    TransformFunction transform = SV_TRANSFORMS.get(dataType).get(RANDOM.nextInt(SV_TRANSFORMS.get(dataType).size()));

    return transform.generateTransform(table, col);
  }

  public static Pair<String, String> generateTransformForMvColumn(String table, FieldSpec col) {
    FieldSpec.DataType dataType = col.getDataType();
    if (!MV_TRANSFORMS.containsKey(dataType)) {
      return Pair.of(col.getName(), "\"" + table + "\".\"" + col.getName() + "\"");
    }

    TransformFunction transform = MV_TRANSFORMS.get(dataType).get(RANDOM.nextInt(MV_TRANSFORMS.get(dataType).size()));
    return transform.generateTransform(table, col);
  }
}
