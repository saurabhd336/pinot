package org.apache.pinot.integration.tests.transformAggregationGenerator;

import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.data.FieldSpec;


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
public class Transforms {
  public static final List<TransformFunction> FUNCTIONS =
      List.of(new SimpleStringTransforms(), new SimpleNumericTransforms(), new SimpleBooleanTransforms(),
          new SimpleNumbericMvTransforms());

  public static class SimpleStringTransforms implements TransformFunction {
    private static final Random _random = new Random();
    private static final List<String> _supportedTransforms =
        List.of("reverse", "lower", "upper", "trim", "ltrim", "rtrim", "toUtf8", "toAscii", "normalize");

    @Override
    public boolean isSvSupported() {
      return true;
    }

    @Override
    public boolean isMvSupported() {
      return false;
    }

    @Override
    public List<FieldSpec.DataType> getSupportedInputDataTypes() {
      return List.of(FieldSpec.DataType.STRING);
    }

    @Override
    public Pair<String, String> generateTransform(String tableName, FieldSpec col) {
      String tfName = _supportedTransforms.get(_random.nextInt(_supportedTransforms.size()));
      return Pair.of(tfName + "(" + col.getName() + ")", tfName + "(\"" + tableName + "\".\"" + col.getName() + "\")");
    }
  }

  public static class SimpleNumericTransforms implements TransformFunction {
    private static final Random _random = new Random();
    private static final List<String> _supportedTransforms =
        List.of("abs", "ceil", "floor", "exp", "ln", "log2", "log10", "sqrt", "sign", "roundDecimal", "sin", "cos",
            "tan", "cot", "asin", "acos", "atan", "sinh", "cosh", "tanh", "degrees", "radians", "mod", "abs", "sign", "truncate");

    @Override
    public boolean isSvSupported() {
      return true;
    }

    @Override
    public boolean isMvSupported() {
      return false;
    }

    @Override
    public List<FieldSpec.DataType> getSupportedInputDataTypes() {
      return List.of(FieldSpec.DataType.INT, FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT,
          FieldSpec.DataType.DOUBLE);
    }

    @Override
    public Pair<String, String> generateTransform(String tableName, FieldSpec col) {
      String tfName = _supportedTransforms.get(_random.nextInt(_supportedTransforms.size()));
      return Pair.of(tfName + "(" + col.getName() + ")", tfName + "(\"" + tableName + "\".\"" + col.getName() + "\")");
    }
  }

  public static class SimpleBooleanTransforms implements TransformFunction {
    private static final Random _random = new Random();
    private static final List<String> _supportedTransforms = List.of("not");

    @Override
    public boolean isSvSupported() {
      return true;
    }

    @Override
    public boolean isMvSupported() {
      return false;
    }

    @Override
    public List<FieldSpec.DataType> getSupportedInputDataTypes() {
      return List.of(FieldSpec.DataType.BOOLEAN);
    }

    @Override
    public Pair<String, String> generateTransform(String tableName, FieldSpec col) {
      String tfName = _supportedTransforms.get(_random.nextInt(_supportedTransforms.size()));
      return Pair.of(tfName + "(" + col.getName() + ")", tfName + "(\"" + tableName + "\".\"" + col.getName() + "\")");
    }
  }

  public static class SimpleNumbericMvTransforms implements TransformFunction {
    private static final Random _random = new Random();
    private static final List<String> _supportedTransforms =
        List.of("arrayLength", "arrayAverage", "arrayMin", "arrayMax", "arraySum");

    @Override
    public boolean isSvSupported() {
      return false;
    }

    @Override
    public boolean isMvSupported() {
      return true;
    }

    @Override
    public List<FieldSpec.DataType> getSupportedInputDataTypes() {
      return List.of(FieldSpec.DataType.INT, FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT,
          FieldSpec.DataType.DOUBLE);
    }

    @Override
    public Pair<String, String> generateTransform(String tableName, FieldSpec col) {
      String tfName = _supportedTransforms.get(_random.nextInt(_supportedTransforms.size()));
      return Pair.of(tfName + "(" + col.getName() + ")", tfName + "(\"" + tableName + "\".\"" + col.getName() + "\")");
    }
  }
}
