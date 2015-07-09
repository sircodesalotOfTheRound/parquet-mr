package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.query.collections.ParquelCollection;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.column.ParquelColumnExpression;
import org.apache.parquet.parqour.query.expressions.infix.ParquelInfixExpression;
import org.apache.parquet.parqour.query.expressions.column.ParquelNamedColumnExpression;
import org.apache.parquet.parqour.query.expressions.column.ParquelWildcardExpression;
import org.apache.parquet.parqour.query.expressions.pql.ParquelTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.tables.ParquelNamedTableExpression;
import org.apache.parquet.parqour.query.expressions.tables.ParquelQuotedTableExpression;
import org.apache.parquet.parqour.query.expressions.tables.ParquelTableExpression;
import org.apache.parquet.parqour.query.expressions.tables.ParquelTableExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.expressions.pql.ParquelFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.pql.ParquelSelectStatement;
import org.apache.parquet.parqour.query.expressions.pql.ParquelWhereExpression;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TestTextQuery {
  @Test
  public void testRootExpression() {
    ParquelLexer lexer = new ParquelLexer("select one, two, three from sometable", true);
    ParquelTreeRootExpression root = new ParquelTreeRootExpression(lexer);

    ParquelCollection<ParquelExpression> expressions = root.expressions();

    assertEquals(1, expressions.count());
    assertTrue(expressions.all(new Predicate<ParquelExpression>() {
      @Override
      public boolean test(ParquelExpression expression) {
        return expression.is(ParquelExpressionType.SELECT);
      }
    }));

    assertTrue(root.containsSelectExpression());

    ParquelSelectStatement select = root.asSelectStatement();
    final Set<String> columns = new HashSet<String>() {{
      add("one");
      add("two");
      add("three");
    }};

    assertEquals(columns.size(), select.columnSet().columns().count());

    // Columns -> NamedColumns -> NamedColumn.toString() -> All(name in columns-hash).
    assertTrue(select.columnSet().columns()
      .map(new Projection<ParquelColumnExpression, ParquelNamedColumnExpression>() {
        @Override
        public ParquelNamedColumnExpression apply(ParquelColumnExpression expression) {
          return (ParquelNamedColumnExpression) expression;
        }
      })
      .map(new Projection<ParquelNamedColumnExpression, String>() {
        @Override
        public String apply(ParquelNamedColumnExpression expression) {
          return expression.identifier().toString();
        }
      })
      .all(new Predicate<String>() {
        @Override
        public boolean test(String columnName) {
          return columns.contains(columnName);
        }
      }));
  }

  @Test
  public void testSelectExpression() {
    ParquelLexer lexer = new ParquelLexer("select *, first, second, third, fourth from table1, table2", true);
    ParquelTreeRootExpression root = new ParquelTreeRootExpression(lexer);

    assertTrue(root.isSelectStatement());

    ParquelSelectStatement select = (ParquelSelectStatement) root.expressions().first();

    final Set<String> columnNames = fillSet("first", "second", "third", "fourth");
    final Set<String> tableNames = fillSet("table1", "table2");

    assert (select.columnSet().columns().count() == 5);
    assert (select.columnSet().columns().ofType(ParquelWildcardExpression.class).count() == 1);
    assert (select.columnSet().columns().ofType(ParquelNamedColumnExpression.class).count() == 4);
    assert (select.from().tableSet().tables().count() == 2);

    boolean areAllColumnsContainedInTheSetAbove = select.columnSet().columns()
      .ofType(ParquelNamedColumnExpression.class)
      .all(new Predicate<ParquelNamedColumnExpression>() {
        @Override
        public boolean test(ParquelNamedColumnExpression column) {
          return columnNames.contains(column.identifier().toString());
        }
      });

    boolean areAllTablesContainedInTheSetAbove = select.from().tableSet().tables()
      .ofType(ParquelNamedTableExpression.class)
      .all(new Predicate<ParquelNamedTableExpression>() {
        @Override
        public boolean test(ParquelNamedTableExpression table) {
          return tableNames.contains(table.fullyQualifiedName().toString());
        }
      });

    assertTrue(areAllColumnsContainedInTheSetAbove);
    assertTrue(areAllTablesContainedInTheSetAbove);
  }

  private Set<String> fillSet(String ... items) {
    Set<String> set = new HashSet<String>();
    for (String item : items) {
      set.add(item);
    }

    return set;
  }

  @Test
  public void testAsFQNExpression() {
    ParquelLexer lexer = new ParquelLexer("file.parq", true);
    ParquelTreeRootExpression root = new ParquelTreeRootExpression(lexer);

    assertTrue(root.isFqnExpression());
    ParquelFullyQualifiedNameExpression path = root.asFqnExpression();

    assertEquals("file.parq", path.toString());


  }
  @Test
  public void testFromFQNExpression() {
    ParquelLexer lexer = new ParquelLexer("select one, two, three from sometable.parq", true);
    ParquelTreeRootExpression root = new ParquelTreeRootExpression(lexer);

    ParquelNamedTableExpression tableExpression = root.asSelectStatement()
      .from()
      .tableSet()
      .tables()
      .firstAs(ParquelNamedTableExpression.class);

    String tableName = tableExpression.fullyQualifiedName().toString();
    assertEquals("sometable.parq", tableName);
  }

  @Test
  public void testFromQuotedTableExpression() {
    for (String path : new String[] { "one.parq", "two three.parq", "four   five.parq", "relative/path/file.parq", "/absolute/path/item.parq" }) {
      String queryExpression = String.format("select * from '%s'", path);
      ParquelLexer lexer = new ParquelLexer(queryExpression, true);
      ParquelTreeRootExpression root = new ParquelTreeRootExpression(lexer);

      ParquelTableExpression tableExpression = root.asSelectStatement()
        .from()
        .tableSet()
        .tables()
        .first();

      assertTrue(tableExpression.tableExpressionType() == ParquelTableExpressionType.QUOTED);
      ParquelQuotedTableExpression quotedTableExpression = tableExpression.asQuotedTableExpression();

      String tableName = quotedTableExpression.asString();
      assertEquals(path, tableName);
    }
  }

  @Test
  public void testWhereExpression() {
    for (String lhs : new String[] { "one", "two.three", "four", "five.six", "6", "10" }) {
      for (String rhs : new String[] {"100", "four.five", "seven.eight.nine.ten", "some", "12", "10" }) {
        for (String operator : new String[]{ "=", "!=", "<", ">" }) {
          String statement = String.format("select * from something where %s %s %s", lhs, operator, rhs);
          ParquelLexer lexer = new ParquelLexer(statement, true);
          ParquelTreeRootExpression rootExpression = new ParquelTreeRootExpression(lexer);

          ParquelSelectStatement selectStatement = rootExpression.asSelectStatement();
          assertTrue(selectStatement.columnSet().containsWildcardColumn());

          assertWhereIsLike(selectStatement.where(), lhs, operator, rhs);
        }
      }
    }
  }

  public void assertWhereIsLike(ParquelWhereExpression whereExpression, String lhs, String operator, String rhs) {
    ParquelInfixExpression infixExpression = whereExpression.infixExpression();

    assertEquals(getLexedTypeForString(lhs), infixExpression.lhs().type());
    assertEquals(getLexedTypeForString(rhs), infixExpression.rhs().type());

    assertEquals(lhs, infixExpression.lhs().toString());
    assertEquals(operator, infixExpression.operator().toString());
    assertEquals(rhs, infixExpression.rhs().toString());
  }

  public ParquelExpressionType getLexedTypeForString(String string) {
    if (Character.isAlphabetic(string.charAt(0))) return ParquelExpressionType.FQN;
    if (Character.isDigit(string.charAt(0))) return ParquelExpressionType.NUMERIC;

    throw new NotImplementedException();
  }
}
