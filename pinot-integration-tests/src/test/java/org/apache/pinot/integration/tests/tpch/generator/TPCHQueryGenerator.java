package org.apache.pinot.integration.tests.tpch.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;


public class TPCHQueryGenerator {
  private final SampleColumnDataProvider _sampleColumnDataProvider;
  private static Map<String, Table> tables = new HashMap<>();
  private static List<String> tableNames =
      List.of("nation", "region", "supplier", "customer", "part", "partsupp", "orders", "lineitem");
  private static final String[] joinTypes = {
      "INNER JOIN", "LEFT JOIN", "RIGHT JOIN"
  };

  public TPCHQueryGenerator() {
    _sampleColumnDataProvider = null;
  }

  public TPCHQueryGenerator(SampleColumnDataProvider sampleColumnDataProvider) {
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
    addRelation("customer", "nation", "c_nationkey", "n_nationkey");
    addRelation("orders", "customer", "o_custkey", "c_custkey");
    addRelation("lineitem", "orders", "l_orderkey", "o_orderkey");
    addRelation("lineitem", "part", "l_partkey", "p_partkey");
    addRelation("lineitem", "supplier", "l_suppkey", "s_suppkey");

    if (_sampleColumnDataProvider != null) {
      tables.forEach((tableName, table) -> {
        table.getColumns().forEach(column -> {
          column.setSampleValues(_sampleColumnDataProvider.getSampleValues(tableName, column.getColumnName()));
        });
      });
    }
  }

  private static Table getRandomTable() {
    Random random = new Random();
    int index = random.nextInt(tables.size());
    return tables.get(tableNames.get(index));
  }

  private void addRandomProjections(StringBuilder query, Table t1) {
    Random random = new Random();
    int numColumns = random.nextInt(t1.getColumns().size()) + 1;
    List<String> selectedColumns = new ArrayList<>();

    while (selectedColumns.size() < numColumns) {
      String columnName = t1.getColumns().get(random.nextInt(t1.getColumns().size())).getColumnName();
      if (!selectedColumns.contains(columnName)) {
        selectedColumns.add(columnName);
        query.append(" \"" + t1.getTableName() + "\".\"" + columnName + "\" ");
        if (selectedColumns.size() < numColumns) {
          query.append(", ");
        }
      }
    }
  }

  public String generateSelectionOnlyQuery(boolean includePredicates) {
    Table t1 = getRandomTable();
    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    addRandomProjections(query, t1);
    query.append(" FROM " + t1.getTableName());

    if (includePredicates) {
      query.append(" WHERE ");
      addRandomPredicates(query, t1);
    }

    return query.toString();
  }

  private void addRandomPredicates(StringBuilder query, Table t1) {
    // todo this should use random values from actual data
    Random random = new Random();
    int predicateCount = random.nextInt(5) + 1;
    List<String> predicates = new ArrayList<>();
    while (predicates.size() < predicateCount) {
      Column column = t1.getColumns().get(random.nextInt(t1.getColumns().size()));
      predicates.add(column.getColumnName());
      String name = column.getColumnName();
      ColumnType columnType = column.getColumnType();
      String operator = columnType.operators.get(random.nextInt(columnType.operators.size()));
      query.append(" \"").append(t1.getTableName()).append("\".\"").append(name).append("\" ");
      query.append(operator);
      if (columnType == ColumnType.STRING) {
        query.append(" '").append(column.getRandomStringValue()).append("' ");
      } else {
        query.append(" ").append(column.getRandomNumericValue()).append(" ");
      }
      if (predicates.size() < predicateCount) {
        query.append(" AND ");
      }
    }
  }

  public String selectionOnlyWithJoins(boolean includePredicates) {
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
    StringBuilder query = new StringBuilder();
    query.append("SELECT ");

    addRandomProjections(query, t1);
    query.append(", ");
    addRandomProjections(query, t2);
    query.append(" FROM " + t1.getTableName());
    query.append(" " + joinTypes[random.nextInt(joinTypes.length)] + " ");
    query.append(" " + t2.getTableName() + " ON ");
    query.append(" \"" + t1.getTableName() + "\".\"" + rt.getLocalTableKey() + "\" ");
    query.append(" = ");
    query.append(" \"" + t2.getTableName() + "\".\"" + rt.getForeignTableKey() + "\" ");

    if (includePredicates) {
      query.append(" WHERE ");
      addRandomPredicates(query, t1);
      query.append(" AND ");
      addRandomPredicates(query, t2);
    }

    return query.toString();
  }

  private List<String> addGroupByAndAggregates(StringBuilder query, Table t1) {
    Random random = new Random();
    int numColumns = random.nextInt(t1.getColumns().size()) + 1;
    List<String> selectedColumns = new ArrayList<>();
    List<String> groupByColumns = new ArrayList<>();

    while (selectedColumns.size() < numColumns) {
      Column column = t1.getColumns().get(random.nextInt(t1.getColumns().size()));
      String columnName = column.getColumnName();
      if (!selectedColumns.contains(columnName)) {
        if (random.nextInt() % 2 == 0 && column.getColumnType().aggregations.size() > 0) {
          // Use as aggregation
          String aggregation = column.getColumnType().aggregations.get(random.nextInt(column.getColumnType().aggregations.size()));
          query.append(aggregation).append("(\"").append(t1.getTableName()).append("\".\"").append(columnName).append("\")");
        } else {
          // Use as group by
          groupByColumns.add(columnName);
          query.append(" \"" + t1.getTableName() + "\".\"" + columnName + "\" ");
        }
        selectedColumns.add(columnName);
        if (selectedColumns.size() < numColumns) {
          query.append(", ");
        }
      }
    }

    return groupByColumns;
  }

  public String selectionOnlyWithGroupBy(boolean includePredicates) {
    Table t1 = getRandomTable();
    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    List<String> groupByColumns = addGroupByAndAggregates(query, t1);

    query.append(" FROM " + t1.getTableName());

    if (includePredicates) {
      query.append(" WHERE ");
      addRandomPredicates(query, t1);
    }

    if (groupByColumns.size() > 0) {
      query.append(" GROUP BY ");
      for (int i = 0; i < groupByColumns.size(); i++) {
        query.append(" \"" + t1.getTableName() + "\".\"" + groupByColumns.get(i) + "\" ");
        if (i < groupByColumns.size() - 1) {
          query.append(", ");
        }
      }
    }

    return query.toString();
  }

  public String selectionOnlyGroupByWithJoins(boolean includePredicates) {
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
    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    List<String> groupByColumns = addGroupByAndAggregates(query, t1);
    query.append(", ");
    List<String> groupByColumnsT2 = addGroupByAndAggregates(query, t2);

    query.append(" FROM " + t1.getTableName());
    query.append(" " + joinTypes[random.nextInt(joinTypes.length)] + " ");
    query.append(" " + t2.getTableName() + " ON ");
    query.append(" \"" + t1.getTableName() + "\".\"" + rt.getLocalTableKey() + "\" ");
    query.append(" = ");
    query.append(" \"" + t2.getTableName() + "\".\"" + rt.getForeignTableKey() + "\" ");

    if (includePredicates) {
      query.append(" WHERE ");
      addRandomPredicates(query, t1);
      query.append(" AND ");
      addRandomPredicates(query, t2);
    }

    if (groupByColumns.size() > 0 || groupByColumnsT2.size() > 0) {
      query.append(" GROUP BY ");
      for (int i = 0; i < groupByColumns.size(); i++) {
        query.append(" \"" + t1.getTableName() + "\".\"" + groupByColumns.get(i) + "\" ");
        if (i < groupByColumns.size() - 1) {
          query.append(", ");
        }
      }

      if (groupByColumnsT2.size() > 0) {
        query.append(", ");
      }

      for (int i = 0; i < groupByColumnsT2.size(); i++) {
        query.append(" \"" + t2.getTableName() + "\".\"" + groupByColumnsT2.get(i) + "\" ");
        if (i < groupByColumnsT2.size() - 1) {
          query.append(", ");
        }
      }
    }

    return query.toString();
  }

  public String generateRandomQuery() {
    Random random = new Random();
    int queryType = random.nextInt(8);
    switch (queryType) {
      case 0:
        return generateSelectionOnlyQuery(false);
      case 1:
        return generateSelectionOnlyQuery(true);
      case 2:
        return selectionOnlyWithJoins(false);
      case 3:
        return selectionOnlyWithJoins(true);
      case 4:
        return selectionOnlyWithGroupBy(false);
      case 5:
        return selectionOnlyWithGroupBy(true);
      case 6:
        return selectionOnlyGroupByWithJoins(false);
      case 7:
        return selectionOnlyGroupByWithJoins(true);
      default:
        return generateSelectionOnlyQuery(false);
    }
  }

  private static int SELECTION_ONLY_QUEIES = 2;
  private static int SELECTION_ONLY_WITH_JOINS_QUERIES = 2;
  private static int SELECTION_ONLY_GROUP_BY_QUERIES = 2;
  private static int SELECTION_ONLY_GROUP_BY_WITH_PREDICATES_QUERIES = 2;

  public static void main(String[] args) {
    TPCHQueryGenerator tpchQueryGenerator = new TPCHQueryGenerator();
    tpchQueryGenerator.init();

    for (int i = 0; i < SELECTION_ONLY_QUEIES; i++) {
      printQuery(tpchQueryGenerator.generateSelectionOnlyQuery(false));
    }

    for (int i = 0; i < SELECTION_ONLY_QUEIES; i++) {
      printQuery(tpchQueryGenerator.generateSelectionOnlyQuery(true));
    }

    for (int i = 0; i < SELECTION_ONLY_WITH_JOINS_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyWithJoins(false));
    }

    for (int i = 0; i < SELECTION_ONLY_WITH_JOINS_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyWithJoins(true));
    }

    for (int i = 0; i < SELECTION_ONLY_GROUP_BY_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyWithGroupBy(false));
    }

    for (int i = 0; i < SELECTION_ONLY_GROUP_BY_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyWithGroupBy(true));
    }

    for (int i = 0; i < SELECTION_ONLY_GROUP_BY_WITH_PREDICATES_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyGroupByWithJoins(false));
    }

    for (int i = 0; i < SELECTION_ONLY_GROUP_BY_WITH_PREDICATES_QUERIES; i++) {
      printQuery(tpchQueryGenerator.selectionOnlyGroupByWithJoins(true));
    }
  }

  private static void printQuery(String query) {
    System.out.printf("%s\n\n", query);
  }
}
