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
package org.apache.pinot.integration.tests.tpch.generator;

import java.util.ArrayList;
import java.util.List;


public class QuerySkeleton {
  private final List<String> projections;
  private final List<String> predicates;
  private final List<String> groupByColumns;
  private final List<String> orderByColumns;
  private final List<String> tables;

  public QuerySkeleton() {
    projections = new ArrayList<>();
    predicates = new ArrayList<>();
    groupByColumns = new ArrayList<>();
    orderByColumns = new ArrayList<>();
    tables = new ArrayList<>();
  }

  public void addTable(String table) {
    tables.add(table);
  }

  public void addProjection(String projection) {
    projections.add(projection);
  }

  public QuerySkeleton addPredicate(String predicate) {
    predicates.add(predicate);
    return this;
  }

  public QuerySkeleton addGroupByColumn(String groupByColumn) {
    groupByColumns.add(groupByColumn);
    return this;
  }

  public QuerySkeleton addOrderByColumn(String orderByColumn) {
    orderByColumns.add(orderByColumn);
    return this;
  }

  @Override
  public String toString() {
    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    projections.forEach(predicate -> query.append(predicate).append(", "));
    query.delete(query.length() - 2, query.length());

    query.append(" FROM ");
    tables.forEach(table -> query.append(table).append(", "));
    query.delete(query.length() - 2, query.length());

    if (predicates.size() > 0) {
      query.append(" WHERE ");
      predicates.forEach(predicate -> query.append(predicate).append(" AND "));
      query.delete(query.length() - 5, query.length());
    }

    if (groupByColumns.size() > 0) {
      query.append(" GROUP BY ");
      groupByColumns.forEach(groupByColumn -> query.append(groupByColumn).append(", "));
      query.delete(query.length() - 2, query.length());
    }

    if (orderByColumns.size() > 0) {
      query.append(" ORDER BY ");
      orderByColumns.forEach(orderByColumn -> query.append(orderByColumn).append(", "));
      query.delete(query.length() - 2, query.length());
    }

    return query.toString();
  }
}
