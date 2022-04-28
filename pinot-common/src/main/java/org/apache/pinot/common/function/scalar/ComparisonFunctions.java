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
package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;

public class ComparisonFunctions {

  private static final double DOUBLE_COMPARISON_TOLERANCE = 1e-7d;

  private ComparisonFunctions() {
  }

  @ScalarFunction(names = {"GREATER_THAN", "gt"})
  public static boolean greaterThan(double a, double b) {
    return a > b;
  }

  @ScalarFunction(names = {"GREATER_THAN_OR_EQUAL", "gte"})
  public static boolean greaterThanOrEquals(double a, double b) {
    return a >= b;
  }

  @ScalarFunction(names = {"LESS_THAN", "lt"})
  public static boolean lessThan(double a, double b) {
    return a < b;
  }

  @ScalarFunction(names = {"LESS_THAN_OR_EQUAL", "lte"})
  public static boolean lessThanOrEquals(double a, double b) {
    return a <= b;
  }

  @ScalarFunction(names = {"NOT_EQUALS", "neq"})
  public static boolean notEquals(double a, double b) {
    return (Math.abs(a - b) >= DOUBLE_COMPARISON_TOLERANCE);
  }

  @ScalarFunction(names = {"EQUALS", "eq"})
  public static boolean equals(double a, double b) {
    // To avoid approximation errors
    return (Math.abs(a - b) < DOUBLE_COMPARISON_TOLERANCE);
  }

  @ScalarFunction(names = {"BETWEEN", "bw"})
  public static boolean between(double val, double a, double b) {
    return (val > a) && (val < b);
  }
}
