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
package org.apache.pinot.segment.local.segment.readers;

import java.math.BigDecimal;
import java.util.Random;
import org.apache.pinot.spi.data.FieldSpec;


public class DataAnonymizer {
  private static final String ALL_CHARS = "-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

  public static Object anonymize(Object originalValue, FieldSpec.DataType dataType) {
    Random rand;
    switch (dataType) {
      case INT:
        rand = new Random((int) originalValue);
        return Math.abs(rand.nextInt());
      case LONG:
        rand = new Random((long) originalValue);
        return Math.abs(rand.nextLong());
      case FLOAT:
        rand = new Random((long)((float) originalValue));
        return Math.abs(rand.nextFloat());
      case DOUBLE:
        rand = new Random((long)((double) originalValue));
        return Math.abs(rand.nextDouble());
      case BIG_DECIMAL:
        rand = new Random(((BigDecimal) originalValue).longValue());
        return BigDecimal.valueOf(Math.abs(rand.nextDouble()));
      case STRING:
        String data = (String) originalValue;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length(); i++) {
          Random r = new Random(data.charAt(i));
          sb.append(ALL_CHARS.charAt(r.nextInt(ALL_CHARS.length())));
        }
        return sb.toString();
      case BYTES:
        byte[] bytes = (byte[]) originalValue;
        byte[] newBytes = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
          Random r = new Random(bytes[i]);
          newBytes[i] = (byte) r.nextInt();
        }
        return newBytes;
    }

    throw new IllegalStateException("Unsupported data type: " + dataType);
  }
}
