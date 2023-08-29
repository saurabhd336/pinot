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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.spi.data.FieldSpec;


public class AvroDataGenerator {
  private static final String DATA_PATH = "/Users/saurabh.dubey/Documents/workspace/stuff/transformsAggregationsTest/";
  private static final String SCHEMA_FILE_PATH =
      "/Users/saurabh.dubey/Documents/workspace/stuff/transformsAggregationsTest/schema.json";
  private static final int NUM_RECORDS = 10000;
  private static final Random RANDOM = new Random();

  public void generateData(org.apache.pinot.spi.data.Schema pinotSchema)
      throws IOException {
    SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("data").fields();

    pinotSchema.getAllFieldSpecs().forEach(fs -> {
      switch (fs.getDataType()) {
        case INT:
          if (fs.isSingleValueField()) {
            schemaFields.name(fs.getName()).type().intType().noDefault();
          } else {
            schemaFields.name(fs.getName()).type().array().items().intType().noDefault();
          }
          break;
        case LONG:
          if (fs.isSingleValueField()) {
            schemaFields.name(fs.getName()).type().longType().noDefault();
          } else {
            schemaFields.name(fs.getName()).type().array().items().longType().noDefault();
          }
          break;
        case FLOAT:
          if (fs.isSingleValueField()) {
            schemaFields.name(fs.getName()).type().floatType().noDefault();
          } else {
            schemaFields.name(fs.getName()).type().array().items().floatType().noDefault();
          }
          break;
        case DOUBLE:
          if (fs.isSingleValueField()) {
            schemaFields.name(fs.getName()).type().doubleType().noDefault();
          } else {
            schemaFields.name(fs.getName()).type().array().items().doubleType().noDefault();
          }
          break;
        case STRING:
          if (fs.isSingleValueField()) {
            schemaFields.name(fs.getName()).type().stringType().noDefault();
          } else {
            schemaFields.name(fs.getName()).type().array().items().stringType().noDefault();
          }
          break;
        case BYTES:
          if (fs.isSingleValueField()) {
            schemaFields.name(fs.getName()).type().bytesType().noDefault();
          } else {
            schemaFields.name(fs.getName()).type().array().items().bytesType().noDefault();
          }
          break;
        case BOOLEAN:
          if (fs.isSingleValueField()) {
            schemaFields.name(fs.getName()).type().intType().noDefault();
          } else {
            schemaFields.name(fs.getName()).type().array().items().booleanType().noDefault();
          }
          break;
      }
    });

    Schema avroSchema = schemaFields.endRecord();

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    Path avroFilePath = Paths.get(DATA_PATH + "transformsAggregationsTestData.avro");
    Files.deleteIfExists(avroFilePath);
    OutputStream outputStream = Files.newOutputStream(avroFilePath);

    dataFileWriter.create(avroSchema, outputStream);

    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      avroSchema.getFields().forEach(f -> {
        record.put(f.name(), getRandomValue(pinotSchema.getFieldSpecFor(f.name())));
      });
      dataFileWriter.append(record);
    }

    dataFileWriter.close();
  }

  public Object getRandomValue(FieldSpec fs) {
    switch (fs.getDataType()) {
      case INT:
        if (fs.isSingleValueField()) {
          return RANDOM.nextInt();
        } else {
          int length = RANDOM.nextInt(10);
          List<Integer> values = new ArrayList<>();
          for (int i = 0; i < length; i++) {
            values.add(RANDOM.nextInt());
          }
          return values;
        }
      case LONG:
        if (fs.isSingleValueField()) {
          return RANDOM.nextLong();
        } else {
          int length = RANDOM.nextInt(10);
          List<Long> values = new ArrayList<>();
          for (int i = 0; i < length; i++) {
            values.add(RANDOM.nextLong());
          }
          return values;
        }
      case FLOAT:
        if (fs.isSingleValueField()) {
          return RANDOM.nextFloat();
        } else {
          int length = RANDOM.nextInt(10);
          List<Float> values = new ArrayList<>();;
          for (int i = 0; i < length; i++) {
            values.add(RANDOM.nextFloat());
          }
          return values;
        }
      case DOUBLE:
        if (fs.isSingleValueField()) {
          return RANDOM.nextDouble();
        } else {
          int length = RANDOM.nextInt(10);
          List<Double> values = new ArrayList<>();
          for (int i = 0; i < length; i++) {
            values.add(RANDOM.nextDouble());
          }
          return values;
        }
      case STRING:
        if (fs.isSingleValueField()) {
          return UUID.randomUUID().toString();
        } else {
          int length = RANDOM.nextInt(10);
          List<String> values = new ArrayList<>();
          for (int i = 0; i < length; i++) {
            values.add(UUID.randomUUID().toString());
          }
          return values;
        }
      case BYTES:
        if (fs.isSingleValueField()) {
          return ByteBuffer.wrap(UUID.randomUUID().toString().getBytes());
        } else {
          int length = RANDOM.nextInt(10);
          List<ByteBuffer> values = new ArrayList<>();
          for (int i = 0; i < length; i++) {
            values.add(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()));
          }
          return values;
        }
      case BOOLEAN:
        if (fs.isSingleValueField()) {
          return RANDOM.nextInt(2);
        } else {
          int length = RANDOM.nextInt(10);
          List<Integer> values = new ArrayList<>();
          for (int i = 0; i < length; i++) {
            values.add(RANDOM.nextInt(2));
          }
          return values;
        }
    }

    return null;
  }

  public static void main(String[] args)
      throws Exception {
    org.apache.pinot.spi.data.Schema pinotSchema =
        org.apache.pinot.spi.data.Schema.fromFile(new File(SCHEMA_FILE_PATH));
    new AvroDataGenerator().generateData(pinotSchema);
  }
}