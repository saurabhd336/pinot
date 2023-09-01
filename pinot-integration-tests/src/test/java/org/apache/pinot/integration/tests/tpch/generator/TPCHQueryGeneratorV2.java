package org.apache.pinot.integration.tests.tpch.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jettison.json.JSONException;


public class TPCHQueryGeneratorV2 {
  private final SampleColumnDataProvider _sampleColumnDataProvider;
  private static Map<String, Table> tables = new HashMap<>();
  private final Random _random = new Random();
  private static List<String> tableNames =
      List.of("nation", "region", "supplier", "customer", "part", "partsupp", "orders", "lineitem");
  private static final String[] joinTypes = {
      "INNER JOIN", "LEFT JOIN", "RIGHT JOIN"
  };

  public TPCHQueryGeneratorV2() {
    _sampleColumnDataProvider = null;
  }

  public TPCHQueryGeneratorV2(SampleColumnDataProvider sampleColumnDataProvider) {
    _sampleColumnDataProvider = sampleColumnDataProvider;
  }

  private void addRelation(String t1, String t2, String t1Key, String t2Key) {
    tables.get(t1).addRelation(t2, t2Key, t1Key);
    tables.get(t2).addRelation(t1, t1Key, t2Key);
  }

  public void init() {
    tables.put("nation", new Table("nation",
        List.of(new Column("n_nationkey", ColumnType.NUMERIC), new Column("n_name", ColumnType.STRING),
            new Column("n_regionkey", ColumnType.NUMERIC), new Column("n_comment", ColumnType.STRING))));

    tables.put("region", new Table("region",
        List.of(new Column("r_regionkey", ColumnType.NUMERIC), new Column("r_name", ColumnType.STRING),
            new Column("r_comment", ColumnType.STRING))));

    tables.put("supplier", new Table("supplier",
        List.of(new Column("s_suppkey", ColumnType.NUMERIC), new Column("s_name", ColumnType.STRING),
            new Column("s_address", ColumnType.STRING), new Column("s_nationkey", ColumnType.NUMERIC),
            new Column("s_phone", ColumnType.STRING), new Column("s_acctbal", ColumnType.NUMERIC),
            new Column("s_comment", ColumnType.STRING))));

    tables.put("customer", new Table("customer",
        List.of(new Column("c_custkey", ColumnType.NUMERIC), new Column("c_name", ColumnType.STRING),
            new Column("c_address", ColumnType.STRING), new Column("c_nationkey", ColumnType.NUMERIC),
            new Column("c_phone", ColumnType.STRING), new Column("c_acctbal", ColumnType.NUMERIC),
            new Column("c_mktsegment", ColumnType.STRING), new Column("c_comment", ColumnType.STRING))));

    tables.put("part", new Table("part",
        List.of(new Column("p_partkey", ColumnType.NUMERIC), new Column("p_name", ColumnType.STRING),
            new Column("p_mfgr", ColumnType.STRING), new Column("p_brand", ColumnType.STRING),
            new Column("p_type", ColumnType.STRING), new Column("p_size", ColumnType.NUMERIC),
            new Column("p_container", ColumnType.STRING), new Column("p_retailprice", ColumnType.NUMERIC),
            new Column("p_comment", ColumnType.STRING))));

    tables.put("partsupp", new Table("partsupp",
        List.of(new Column("ps_partkey", ColumnType.NUMERIC), new Column("ps_suppkey", ColumnType.NUMERIC),
            new Column("ps_availqty", ColumnType.NUMERIC), new Column("ps_supplycost", ColumnType.NUMERIC),
            new Column("ps_comment", ColumnType.STRING))));

    tables.put("orders", new Table("orders",
        List.of(new Column("o_orderkey", ColumnType.NUMERIC), new Column("o_custkey", ColumnType.NUMERIC),
            new Column("o_orderstatus", ColumnType.STRING), new Column("o_totalprice", ColumnType.NUMERIC),
            new Column("o_orderdate", ColumnType.STRING), new Column("o_orderpriority", ColumnType.STRING),
            new Column("o_clerk", ColumnType.STRING), new Column("o_shippriority", ColumnType.STRING),
            new Column("o_comment", ColumnType.STRING))));

    tables.put("lineitem", new Table("lineitem",
        List.of(new Column("l_orderkey", ColumnType.NUMERIC), new Column("l_partkey", ColumnType.NUMERIC),
            new Column("l_suppkey", ColumnType.NUMERIC), new Column("l_linenumber", ColumnType.NUMERIC),
            new Column("l_quantity", ColumnType.NUMERIC), new Column("l_extendedprice", ColumnType.NUMERIC),
            new Column("l_discount", ColumnType.NUMERIC), new Column("l_tax", ColumnType.NUMERIC),
            new Column("l_returnflag", ColumnType.STRING), new Column("l_linestatus", ColumnType.STRING),
            new Column("l_shipdate", ColumnType.STRING), new Column("l_commitdate", ColumnType.STRING),
            new Column("l_receiptdate", ColumnType.STRING), new Column("l_shipinstruct", ColumnType.STRING),
            new Column("l_shipmode", ColumnType.STRING), new Column("l_comment", ColumnType.STRING))));

    addRelation("nation", "region", "n_regionkey", "r_regionkey");
    addRelation("supplier", "nation", "s_nationkey", "n_nationkey");
    addRelation("supplier", "customer", "s_nationkey", "c_nationkey");
    addRelation("supplier", "partsupp", "s_suppkey", "ps_suppkey");
    addRelation("customer", "nation", "c_nationkey", "n_nationkey");
    addRelation("orders", "customer", "o_custkey", "c_custkey");
    addRelation("lineitem", "orders", "l_orderkey", "o_orderkey");
    addRelation("lineitem", "part", "l_partkey", "p_partkey");
    addRelation("lineitem", "supplier", "l_suppkey", "s_suppkey");
    addRelation("lineitem", "partsupp", "l_partkey", "ps_partkey");
    addRelation("lineitem", "partsupp", "l_suppkey", "ps_partkey");
    addRelation("part", "partsupp", "p_partkey", "ps_partkey");

    if (_sampleColumnDataProvider != null) {
      tables.forEach((tableName, table) -> {
        table.getColumns().forEach(column -> {
          Pair<Boolean, List<String>> sampleValues = null;
          try {
            sampleValues = _sampleColumnDataProvider.getSampleValues(tableName, column.getColumnName());
          } catch (JSONException e) {
            throw new RuntimeException(e);
          }
          column.setSampleValues(sampleValues.getRight());
          column.setMultiValue(sampleValues.getLeft());
        });
      });
    }
  }

  private static Table getRandomTable() {
    Random random = new Random();
    int index = random.nextInt(tables.size());
    return tables.get(tableNames.get(index));
  }

  private List<String> getRandomProjections(Table t1) {
    Random random = new Random();
    int numColumns = random.nextInt(t1.getColumns().size()) + 1;
    List<String> selectedColumns = new ArrayList<>();

    while (selectedColumns.size() < numColumns) {
      String columnName = t1.getColumns().get(random.nextInt(t1.getColumns().size())).getColumnName();
      if (!selectedColumns.contains(columnName)) {
        selectedColumns.add(columnName);
      }
    }

    return selectedColumns;
  }

  private Pair<String, String> generateInnerQueryForPredicate(Table t1, Column c) {
    QuerySkeleton innerQuery = new QuerySkeleton();
    QuerySkeleton innerQueryPinot = new QuerySkeleton();

    Random random = new Random();
    List<String> predicates = new ArrayList<>();
    List<String> predicatesForPinotQuery = new ArrayList<>();

    innerQuery.addTable(t1.getTableName());
    innerQueryPinot.addTable(t1.getTableName());
    // Limit to maximum of 1 join
    if (random.nextBoolean()) {
      RelatedTable relatedTable = t1.getRelatedTables().get(random.nextInt(t1.getRelatedTables().size()));
      if (relatedTable != null) {
        innerQuery.addTable(relatedTable.getForeignTableName());
        innerQueryPinot.addTable(relatedTable.getForeignTableName());
        predicates.add(relatedTable.getLocalTableKey() + "=" + relatedTable.getForeignTableKey());
        predicatesForPinotQuery.add(relatedTable.getLocalTableKey() + "=" + relatedTable.getForeignTableKey());

        Pair<List<String>, List<String>> inp =
            getRandomPredicates("", tables.get(relatedTable.getForeignTableName()), false);
        predicates.addAll(inp.getLeft());
        predicatesForPinotQuery.addAll(inp.getRight());
      }
    }
    String aggregation = c.getColumnType().aggregations.get(random.nextInt(c.getColumnType().aggregations.size()));
    innerQuery.addProjection(aggregation + "(" + c.getColumnName() + ")");
    innerQueryPinot.addProjection(aggregation + "(" + c.getColumnName() + ")");

    Pair<List<String>, List<String>> inp = getRandomPredicates("", t1, false);

    predicates.addAll(inp.getLeft());
    predicatesForPinotQuery.addAll(inp.getRight());

    predicates.forEach(innerQuery::addPredicate);
    predicatesForPinotQuery.forEach(innerQueryPinot::addPredicate);
    return Pair.of(innerQuery.toString(), innerQueryPinot.toString());
  }

  private Pair<String, String> getRandomValueForPredicate(Table t1, Column c, boolean useNextedQueries) {
    Random random = new Random();
    if (random.nextBoolean() && useNextedQueries && c.getColumnType().aggregations.size() > 0) {
      // Use nested query for predicate
      Pair<String, String> nestedQueries = generateInnerQueryForPredicate(t1, c);
      return Pair.of("(" + nestedQueries.getLeft() + ")", "(" + nestedQueries.getRight() + ")");
    } else {
      if (c.getColumnType() == ColumnType.STRING) {
        String value = "'" + c.getRandomStringValue() + "'";
        return Pair.of(value, value);
      } else {
        String value = String.valueOf(c.getRandomNumericValue());
        return Pair.of(value, value);
      }
    }
  }

  private Pair<List<String>, List<String>> getRandomPredicates(String prefix, Table t1, boolean useNestedQueries) {
    Random random = new Random();
    int predicateCount = random.nextInt(5) + 1;
    List<String> predicates = new ArrayList<>();
    List<String> results = new ArrayList<>();
    List<String> resultsPinot = new ArrayList<>();
    while (predicates.size() < predicateCount) {
      Column column = t1.getColumns().get(random.nextInt(t1.getColumns().size()));
      predicates.add(column.getColumnName());
      String name = column.getColumnNameForPredicate(prefix);
      ColumnType columnType = column.getColumnType();
      String operator = columnType.operators.get(random.nextInt(columnType.operators.size()));
      Pair<String, String> value = getRandomValueForPredicate(t1, column, useNestedQueries);
      String predicateBuilder = name + " " + operator + " " + value.getLeft() + " ";
      results.add(predicateBuilder);
      String predicateBuilderPinot =
          column.getColumnNameForPinotPredicate(prefix) + " " + operator + " " + value.getRight() + " ";
      resultsPinot.add(predicateBuilderPinot);
    }

    return Pair.of(results, resultsPinot);
  }

  private Pair<List<String>, List<String>> getRandomPredicates(Table t1) {
    return getRandomPredicates("", t1, true);
  }

  private List<String> getRandomOrderBys(Table t1) {
    Random random = new Random();
    int orderByCount = random.nextInt(2) + 1;
    List<String> orderBys = new ArrayList<>();
    List<String> results = new ArrayList<>();
    while (orderBys.size() < orderByCount) {
      Column column = t1.getColumns().get(random.nextInt(t1.getColumns().size()));
      orderBys.add(column.getColumnName());
      String name = column.getColumnName();
      StringBuilder orderByBuilder = new StringBuilder();
      orderByBuilder.append(name).append(" ");
      if (random.nextBoolean()) {
        orderByBuilder.append(" DESC ");
      }
      results.add(orderByBuilder.toString());
    }

    return results;
  }

  public Pair<String, String> generateSelectionOnlyQuery(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();
    QuerySkeleton pinotQuerySkeleton = new QuerySkeleton();
    Table t1 = getRandomTable();
    querySkeleton.addTable(t1.getTableName());
    pinotQuerySkeleton.addTable(t1.getTableName());

    getRandomProjections(t1).forEach(p -> {
      querySkeleton.addProjection(p);
      pinotQuerySkeleton.addProjection(p);
    });

    if (includePredicates) {
      Pair<List<String>, List<String>> predicates = getRandomPredicates(t1);
      predicates.getLeft().forEach(querySkeleton::addPredicate);
      predicates.getRight().forEach(pinotQuerySkeleton::addPredicate);
    }

    if (includeOrderBy) {
      getRandomOrderBys(t1).forEach(o -> {
        querySkeleton.addOrderByColumn(o);
        pinotQuerySkeleton.addOrderByColumn(o);
      });
    }

    return Pair.of(querySkeleton.toString(), pinotQuerySkeleton.toString());
  }

  private List<String> getRandomOrderBys(Table t1, List<String> groupByCols) {
    List<String> result = new ArrayList<>();
    if (groupByCols.size() == 0) {
      return result;
    }
    Random random = new Random();
    List<String> orderBys = new ArrayList<>();
    int orderByCount = random.nextInt(groupByCols.size()) + 1;
    while (orderBys.size() < orderByCount) {
      String column = groupByCols.get(random.nextInt(groupByCols.size()));

      if (groupByCols.contains(column)) {
        orderBys.add(column);
        if (random.nextBoolean()) {
          result.add(column + " DESC");
        } else {
          result.add(column);
        }
      }
    }

    return result;
  }

  public Pair<String, String> selectionOnlyWithJoins(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();
    QuerySkeleton pinotQuerySkeleton = new QuerySkeleton();

    Table t1;
    while (true) {
      t1 = getRandomTable();
      if (t1.getRelatedTables().size() > 0) {
        break;
      }
    }

    Random random = new Random();
    RelatedTable rt = t1.getRelatedTables().get(random.nextInt(t1.getRelatedTables().size()));
    Table t2 = tables.get(rt.getForeignTableName());
    getRandomProjections(t1).forEach(p -> {
      querySkeleton.addProjection(p);
      pinotQuerySkeleton.addProjection(p);
    });
    getRandomProjections(t2).forEach(p -> {
      querySkeleton.addProjection(p);
      pinotQuerySkeleton.addProjection(p);
    });

    String t2NameWithJoin =
        t1.getTableName() + " " + joinTypes[random.nextInt(joinTypes.length)] + " " + t2.getTableName() + " ON "
            + rt.getLocalTableKey() + " = " + rt.getForeignTableKey() + " ";
    querySkeleton.addTable(t2NameWithJoin);
    pinotQuerySkeleton.addTable(t2NameWithJoin);

    if (includePredicates) {
      Pair<List<String>, List<String>> predicates = getRandomPredicates(t1);
      predicates.getLeft().forEach(querySkeleton::addPredicate);
      predicates.getRight().forEach(pinotQuerySkeleton::addPredicate);

      predicates = getRandomPredicates(t2);
      predicates.getLeft().forEach(querySkeleton::addPredicate);
      predicates.getRight().forEach(pinotQuerySkeleton::addPredicate);
    }

    if (includeOrderBy) {
      getRandomOrderBys(t1).forEach(o -> {
        querySkeleton.addOrderByColumn(o);
        pinotQuerySkeleton.addOrderByColumn(o);
      });
    }

    return Pair.of(querySkeleton.toString(), pinotQuerySkeleton.toString());
  }

  private Pair<List<String>, List<String>> getGroupByAndAggregates(Table t1) {
    Random random = new Random();
    int numColumns = random.nextInt(t1.getColumns().size()) + 1;
    List<String> selectedColumns = new ArrayList<>();
    List<String> groupByColumns = new ArrayList<>();
    List<String> resultProjections = new ArrayList<>();

    while (selectedColumns.size() < numColumns) {
      Column column = t1.getColumns().get(random.nextInt(t1.getColumns().size()));
      String columnName = column.getColumnName();
      if (!selectedColumns.contains(columnName)) {
        if (random.nextBoolean() && column.getColumnType().aggregations.size() > 0) {
          // Use as aggregation
          String aggregation =
              column.getColumnType().aggregations.get(random.nextInt(column.getColumnType().aggregations.size()));
          resultProjections.add(aggregation + "(" + columnName + ")");
        } else {
          // Use as group by
          groupByColumns.add(columnName);
          resultProjections.add(columnName);
        }
        selectedColumns.add(columnName);
      }
    }

    return Pair.of(resultProjections, groupByColumns);
  }

  public Pair<String, String> selectionOnlyWithGroupBy(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();
    QuerySkeleton pinotQuerySkeleton = new QuerySkeleton();

    Table t1 = getRandomTable();
    Pair<List<String>, List<String>> cols = getGroupByAndAggregates(t1);
    cols.getLeft().forEach(c -> {
      querySkeleton.addProjection(c);
      pinotQuerySkeleton.addProjection(c);
    });

    querySkeleton.addTable(t1.getTableName());
    pinotQuerySkeleton.addTable(t1.getTableName());

    cols.getRight().forEach(c -> {
      querySkeleton.addGroupByColumn(c);
      pinotQuerySkeleton.addGroupByColumn(c);
    });
    if (includePredicates) {
      Pair<List<String>, List<String>> preds = getRandomPredicates(t1);
      preds.getLeft().forEach(querySkeleton::addPredicate);
      preds.getRight().forEach(pinotQuerySkeleton::addPredicate);
    }

    if (includeOrderBy && cols.getRight().size() > 0) {
      getRandomOrderBys(t1, cols.getRight()).forEach(ob -> {
        querySkeleton.addOrderByColumn(ob);
        pinotQuerySkeleton.addOrderByColumn(ob);
      });
    }

    return Pair.of(querySkeleton.toString(), pinotQuerySkeleton.toString());
  }

  public Pair<String, String> selectionOnlyGroupByWithJoins(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();
    QuerySkeleton pinotQuerySkeleton = new QuerySkeleton();
    Table t1;
    while (true) {
      t1 = getRandomTable();
      if (t1.getRelatedTables().size() > 0) {
        break;
      }
    }

    Random random = new Random();
    RelatedTable rt = t1.getRelatedTables().get(random.nextInt(t1.getRelatedTables().size()));
    Table t2 = tables.get(rt.getForeignTableName());
    Pair<List<String>, List<String>> groupByColumns = getGroupByAndAggregates(t1);
    groupByColumns.getLeft().forEach(c -> {
      querySkeleton.addProjection(c);
      pinotQuerySkeleton.addProjection(c);
    });

    Pair<List<String>, List<String>> groupByColumnsT2 = getGroupByAndAggregates(t2);
    groupByColumnsT2.getLeft().forEach(c -> {
      querySkeleton.addProjection(c);
      pinotQuerySkeleton.addProjection(c);
    });

    String tName =
        t1.getTableName() + "  " + joinTypes[random.nextInt(joinTypes.length)] + " " + t2.getTableName() + " ON " + " "
            + rt.getLocalTableKey() + " = " + " " + rt.getForeignTableKey() + " ";

    querySkeleton.addTable(tName);
    pinotQuerySkeleton.addTable(tName);

    groupByColumns.getRight().forEach(c -> {
      querySkeleton.addGroupByColumn(c);
      pinotQuerySkeleton.addGroupByColumn(c);
    });
    groupByColumnsT2.getRight().forEach(c -> {
      querySkeleton.addGroupByColumn(c);
      pinotQuerySkeleton.addGroupByColumn(c);
    });

    if (includePredicates) {
      Pair<List<String>, List<String>> predicates = getRandomPredicates(t1);
      predicates.getLeft().forEach(querySkeleton::addPredicate);
      predicates.getRight().forEach(pinotQuerySkeleton::addPredicate);
    }

    if (includeOrderBy) {
      getRandomOrderBys(t1, groupByColumns.getRight()).forEach(o -> {
        querySkeleton.addOrderByColumn(o);
        pinotQuerySkeleton.addOrderByColumn(o);
      });
      getRandomOrderBys(t2, groupByColumnsT2.getRight()).forEach(o -> {
        querySkeleton.addOrderByColumn(o);
        pinotQuerySkeleton.addOrderByColumn(o);
      });
    }

    return Pair.of(querySkeleton.toString(), pinotQuerySkeleton.toString());
  }

  public Pair<String, String> selectionOnlyMultiJoin(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();
    QuerySkeleton pinotQuerySkeleton = new QuerySkeleton();

    List<String> predicates = new ArrayList<>();
    List<Table> tables = new ArrayList<>();
    Set<String> tableNames = new HashSet<>();

    Random random = new Random();

    // Start off with a random table with related tables
    while (true) {
      Table t1 = getRandomTable();
      if (t1.getRelatedTables().size() > 0) {
        tables.add(t1);
        tableNames.add(t1.getTableName());
        break;
      }
    }

    // Add more tables
    while (random.nextInt() % 8 != 0) {
      int tableToAddIdx = random.nextInt(tables.size());
      RelatedTable relatedTable = tables.get(tableToAddIdx).getRelatedTables()
          .get(random.nextInt(tables.get(tableToAddIdx).getRelatedTables().size()));
      if (!tableNames.contains(relatedTable.getForeignTableName())) {
        tableNames.add(relatedTable.getForeignTableName());
        tables.add(TPCHQueryGeneratorV2.tables.get(relatedTable.getForeignTableName()));
        predicates.add(relatedTable.getLocalTableKey() + "="
                + relatedTable.getForeignTableKey());
      }
    }

    for (Table item : tables) {
      getRandomProjections(item).forEach(p -> {
        querySkeleton.addProjection(p);
        pinotQuerySkeleton.addProjection(p);
      });
    }
    for (Table value : tables) {
      querySkeleton.addTable(value.getTableName());
      pinotQuerySkeleton.addTable(value.getTableName());
    }

    if (predicates.size() > 0) {
      for (String predicate : predicates) {
        querySkeleton.addPredicate(predicate);
        pinotQuerySkeleton.addPredicate(predicate);
      }
    }

    if (includePredicates) {
      for (Table table : tables) {
        Pair<List<String>, List<String>> preds = getRandomPredicates(table);
        preds.getLeft().forEach(querySkeleton::addPredicate);
        preds.getRight().forEach(pinotQuerySkeleton::addPredicate);
      }
    }

    if (includeOrderBy) {
      for (Table table : tables) {
        getRandomOrderBys(table).forEach(o -> {
          querySkeleton.addOrderByColumn(o);
          pinotQuerySkeleton.addOrderByColumn(o);
        });
      }
    }

    return Pair.of(querySkeleton.toString(), pinotQuerySkeleton.toString());
  }

  public Pair<String, String> selectionGroupByMultiJoin(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();
    QuerySkeleton pinotQuerySkeleton = new QuerySkeleton();

    List<String> predicates = new ArrayList<>();
    List<Table> tables = new ArrayList<>();
    Set<String> tableNames = new HashSet<>();

    Random random = new Random();

    // Start off with a random table with related tables
    while (true) {
      Table t1 = getRandomTable();
      if (t1.getRelatedTables().size() > 0) {
        tables.add(t1);
        tableNames.add(t1.getTableName());
        break;
      }
    }

    // Add more tables
    while (random.nextInt() % 8 != 0) {
      int tableToAddIdx = random.nextInt(tables.size());
      RelatedTable relatedTable = tables.get(tableToAddIdx).getRelatedTables()
          .get(random.nextInt(tables.get(tableToAddIdx).getRelatedTables().size()));
      if (!tableNames.contains(relatedTable.getForeignTableName())) {
        tableNames.add(relatedTable.getForeignTableName());
        tables.add(TPCHQueryGeneratorV2.tables.get(relatedTable.getForeignTableName()));
        predicates.add(
            relatedTable.getLocalTableKey() + "=" + relatedTable.getForeignTableKey());
      }
    }

    Map<String, List<String>> tableWiseGroupByCols = new HashMap<>();
    for (Table value : tables) {
      Pair<List<String>, List<String>> groupByAndAggregates = getGroupByAndAggregates(value);
      groupByAndAggregates.getLeft().forEach(g -> {
        querySkeleton.addProjection(g);
        pinotQuerySkeleton.addProjection(g);
      });
      groupByAndAggregates.getRight().forEach(c -> {
        querySkeleton.addGroupByColumn(c);
        pinotQuerySkeleton.addGroupByColumn(c);
      });
      tableWiseGroupByCols.put(value.getTableName(), groupByAndAggregates.getRight());
    }
    for (Table table : tables) {
      querySkeleton.addTable(table.getTableName());
      pinotQuerySkeleton.addTable(table.getTableName());
    }
    predicates.forEach(p -> {
      querySkeleton.addPredicate(p);
      pinotQuerySkeleton.addPredicate(p);
    });

    if (includePredicates) {
      for (Table table : tables) {
        Pair<List<String>, List<String>> preds = getRandomPredicates(table);

        preds.getLeft().forEach(querySkeleton::addPredicate);
        preds.getRight().forEach(pinotQuerySkeleton::addPredicate);
      }
    }

    if (includeOrderBy) {
      for (Table table : tables) {
        getRandomOrderBys(table, tableWiseGroupByCols.get(table.getTableName())).forEach(o -> {
          querySkeleton.addOrderByColumn(o);
          pinotQuerySkeleton.addOrderByColumn(o);
        });
      }
    }

    return Pair.of(querySkeleton.toString(), pinotQuerySkeleton.toString());
  }

//  public Pair<String, String> selectionGroupBySelfJoin(boolean includePredicates, boolean includeOrderBy) {
//    Table t1 = getRandomTable();
//
//    QuerySkeleton querySkeleton = new QuerySkeleton();
//    QuerySkeleton pinotQuerySkeleton = new QuerySkeleton();
//
//    Pair<List<String>, List<String>> groupByAndAggregates = getGroupByAndAggregates(t1);
//    groupByAndAggregates.getLeft().forEach(p -> {
//      querySkeleton.addProjection("t1." + p);
//      pinotQuerySkeleton.addProjection("t1." + p);
//    });
//    groupByAndAggregates.getRight().forEach(g -> {
//      querySkeleton.addGroupByColumn("t1." + g);
//      pinotQuerySkeleton.addGroupByColumn("t1." + g);
//    });
//    String col = t1.getColumns().get(_random.nextInt(t1.getColumns().size())).getColumnName();
//    String tName =
//        t1.getTableName() + " as t1 " + joinTypes[_random.nextInt(joinTypes.length)] + " " + t1.getTableName()
//            + " as t2 ON t1." + col + " = t2." + col + " ";
//    querySkeleton.addTable(tName);
//    pinotQuerySkeleton.addTable(tName);
//
//    if (includeOrderBy) {
//      getRandomOrderBys(t1, groupByAndAggregates.getRight()).forEach(o -> {
//        querySkeleton.addOrderByColumn("t1." + o);
//        pinotQuerySkeleton.addOrderByColumn("t1." + o);
//      });
//    }
//
//    return Pair.of(querySkeleton.toString(), pinotQuerySkeleton.toString());
//  }

//  public Pair<String, String> selectionOnlySelfJoin(boolean includePredicates, boolean includeOrderBy) {
//    Table t1 = getRandomTable();
//    String col = t1.getColumns().get(_random.nextInt(t1.getColumns().size())).getColumnName();
//    QuerySkeleton querySkeleton = new QuerySkeleton();
//    QuerySkeleton pinotQuerySkeleton = new QuerySkeleton();
//
//    getRandomProjections(t1).forEach(p -> {
//      querySkeleton.addProjection("t1." + p);
//      pinotQuerySkeleton.addProjection("t1." + p);
//    });
//    String tName =
//        t1.getTableName() + " as t1 " + joinTypes[_random.nextInt(joinTypes.length)] + " " + t1.getTableName()
//            + " as t2 ON t1." + col + " = t2." + col + " ";
//    querySkeleton.addTable(tName);
//    pinotQuerySkeleton.addTable(tName);
//
//    if (includePredicates) {
//      Pair<List<String>, List<String>> predicates = getRandomPredicates("t1", t1, true);
//      predicates.getLeft().forEach(p -> {
//        querySkeleton.addPredicate(p);
//      });
//      predicates.getRight().forEach(p -> {
//        pinotQuerySkeleton.addPredicate(p);
//      });
//    }
//
//    if (includeOrderBy) {
//      getRandomOrderBys(t1).forEach(o -> {
//        querySkeleton.addOrderByColumn("t1." + o);
//        pinotQuerySkeleton.addOrderByColumn("t1." + o);
//      });
//    }
//
//    return Pair.of(querySkeleton.toString(), pinotQuerySkeleton.toString());
//  }

  public Pair<String, String> generateRandomQuery() {
    Random random = new Random();
    int queryType = random.nextInt(6);
    boolean includePredicates = random.nextBoolean();
    boolean includeOrderBy = true;
    switch (queryType) {
      case 0:
        return generateSelectionOnlyQuery(includePredicates, includeOrderBy);
      case 1:
        return selectionOnlyWithJoins(includePredicates, includeOrderBy);
      case 2:
        return selectionOnlyWithGroupBy(includePredicates, includeOrderBy);
      case 3:
        return selectionOnlyGroupByWithJoins(includePredicates, includeOrderBy);
      case 4:
        return selectionOnlyMultiJoin(includePredicates, includeOrderBy);
      case 5:
        return selectionGroupByMultiJoin(includePredicates, includeOrderBy);
//      case 6:
//        return selectionOnlySelfJoin(includePredicates, includeOrderBy);
//      case 7:
//        return selectionGroupBySelfJoin(includePredicates, includeOrderBy);
      default:
        return generateSelectionOnlyQuery(includePredicates, includeOrderBy);
    }
  }

  private static int SELECTION_ONLY_QUEIES = 1;
  private static int SELECTION_ONLY_WITH_JOINS_QUERIES = 1;
  private static int SELECTION_ONLY_GROUP_BY_QUERIES = 1;
  private static int SELECTION_ONLY_GROUP_BY_WITH_PREDICATES_QUERIES = 1;
  private static int SELECTION_ONLY_MULTI_JOIN_PREDICATES_QUERIES = 1;
  private static int SELECITION_GROUP_BY_MULTI_JOIN_PREDICATES_QUERIES = 1;
  private static int SELECTION_ONLY_SELF_JOIN = 1;
  private static int SELECTION_ONLY_GROUP_BY_SELF_JOIN = 1;

  public static void main(String[] args) {
    TPCHQueryGeneratorV2 tpchQueryGenerator = new TPCHQueryGeneratorV2();
    tpchQueryGenerator.init();

    for (int i = 0; i < SELECTION_ONLY_QUEIES; i++) {
      printQuery(tpchQueryGenerator.generateSelectionOnlyQuery(true, true));
    }

    for (int i = 0; i < SELECTION_ONLY_QUEIES; i++) {
      printQuery(tpchQueryGenerator.generateSelectionOnlyQuery(true, true));
    }

    for (int i = 0; i < SELECTION_ONLY_WITH_JOINS_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyWithJoins(false, true));
    }

    for (int i = 0; i < SELECTION_ONLY_WITH_JOINS_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyWithJoins(true, true));
    }

    for (int i = 0; i < SELECTION_ONLY_GROUP_BY_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyWithGroupBy(true, true));
    }

    for (int i = 0; i < SELECTION_ONLY_GROUP_BY_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyWithGroupBy(true, true));
    }
//
    for (int i = 0; i < SELECTION_ONLY_GROUP_BY_WITH_PREDICATES_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyGroupByWithJoins(true, true));
    }

    for (int i = 0; i < SELECTION_ONLY_GROUP_BY_WITH_PREDICATES_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyGroupByWithJoins(true, true));
    }
//
    for (int i = 0; i < SELECTION_ONLY_MULTI_JOIN_PREDICATES_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyMultiJoin(true, true));
    }

    for (int i = 0; i < SELECITION_GROUP_BY_MULTI_JOIN_PREDICATES_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionGroupByMultiJoin(true, true));
    }

//    for (int i = 0; i < SELECTION_ONLY_SELF_JOIN; i++) {
//      printQuery(tpchQueryGenerator.selectionOnlySelfJoin(true, true));
//    }
//
//    for (int i = 0; i < SELECTION_ONLY_GROUP_BY_SELF_JOIN; i++) {
//      printQuery(tpchQueryGenerator.selectionGroupBySelfJoin(true, true));
//    }
  }

  private static void printQuery(Pair<String, String> query) {
    System.out.printf("%s\n\n", query);
  }
}
