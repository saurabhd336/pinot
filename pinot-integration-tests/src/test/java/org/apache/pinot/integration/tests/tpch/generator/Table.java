package org.apache.pinot.integration.tests.tpch.generator;

import java.util.ArrayList;
import java.util.List;


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
class Table {
  private final String tableName;

  public String getTableName() {
    return tableName;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public List<RelatedTable> getRelatedTables() {
    return relatedTables;
  }

  private final List<Column> columns;
  private final List<RelatedTable> relatedTables;

  public Table(String tableName, List<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
    this.relatedTables = new ArrayList<>();
  }

  public void addRelation(String foreignTableName, String foreignTableKey, String localTableKey) {
    this.relatedTables.add(new RelatedTable(foreignTableName, foreignTableKey, localTableKey));
  }
}
