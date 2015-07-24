package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryWildcardExpression;
import org.apache.parquet.parqour.query.expressions.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQuerySelectStatementExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryWhereExpression;
import org.apache.parquet.parqour.query.expressions.tables.ParquelTableExpressionType;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryNamedTableExpression;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryStringExpression;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryTableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
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
    TextQueryLexer lexer = new TextQueryLexer("select one, two, three from sometable", true);
    TextQueryTreeRootExpression root = new TextQueryTreeRootExpression(lexer);

    TextQueryCollection<TextQueryExpression> expressions = root.expressions();

    assertEquals(1, expressions.count());
    assertTrue(expressions.all(new Predicate<TextQueryExpression>() {
      @Override
      public boolean test(TextQueryExpression expression) {
        return expression.is(TextQueryExpressionType.SELECT);
      }
    }));

    assertTrue(root.containsSelectExpression());

    TextQuerySelectStatementExpression select = root.asSelectStatement();
    final Set<String> columns = new HashSet<String>() {{
      add("one");
      add("two");
      add("three");
    }};

    assertEquals(columns.size(), select.columnSet().columns().count());

    // Columns -> NamedColumns -> NamedColumn.toString() -> All(name in columns-hash).
    assertTrue(select.columnSet().columns()
      .map(new Projection<TextQueryVariableExpression, TextQueryNamedColumnExpression>() {
        @Override
        public TextQueryNamedColumnExpression apply(TextQueryVariableExpression expression) {
          return (TextQueryNamedColumnExpression) expression;
        }
      })
      .map(new Projection<TextQueryNamedColumnExpression, String>() {
        @Override
        public String apply(TextQueryNamedColumnExpression expression) {
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
    TextQueryLexer lexer = new TextQueryLexer("select *, first, second, third, fourth from table1, table2", true);
    TextQueryTreeRootExpression root = new TextQueryTreeRootExpression(lexer);

    assertTrue(root.isSelectStatement());

    TextQuerySelectStatementExpression select = (TextQuerySelectStatementExpression) root.expressions().first();

    final Set<String> columnNames = fillSet("first", "second", "third", "fourth");
    final Set<String> tableNames = fillSet("table1", "table2");

    assert (select.columnSet().columns().count() == 5);
    assert (select.columnSet().columns().ofType(TextQueryWildcardExpression.class).count() == 1);
    assert (select.columnSet().columns().ofType(TextQueryNamedColumnExpression.class).count() == 4);
    assert (select.from().tableSet().tables().count() == 2);

    boolean areAllColumnsContainedInTheSetAbove = select.columnSet().columns()
      .ofType(TextQueryNamedColumnExpression.class)
      .all(new Predicate<TextQueryNamedColumnExpression>() {
        @Override
        public boolean test(TextQueryNamedColumnExpression column) {
          return columnNames.contains(column.identifier().toString());
        }
      });

    boolean areAllTablesContainedInTheSetAbove = select.from().tableSet().tables()
      .ofType(TextQueryNamedTableExpression.class)
      .all(new Predicate<TextQueryNamedTableExpression>() {
        @Override
        public boolean test(TextQueryNamedTableExpression table) {
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
    TextQueryLexer lexer = new TextQueryLexer("file.parq", true);
    TextQueryTreeRootExpression root = new TextQueryTreeRootExpression(lexer);

    assertTrue(root.isFqnExpression());
    TextQueryFullyQualifiedNameExpression path = root.asFqnExpression();

    assertEquals("file.parq", path.toString());


  }
  @Test
  public void testFromFQNExpression() {
    TextQueryLexer lexer = new TextQueryLexer("select one, two, three from 'sometable.parq'", true);
    TextQueryTreeRootExpression root = new TextQueryTreeRootExpression(lexer);

    TextQueryStringExpression tableExpression = root.asSelectStatement()
      .from()
      .tableSet()
      .tables()
      .firstAs(TextQueryStringExpression.class);

    String tableName = tableExpression.asString();
    assertEquals("sometable.parq", tableName);
  }

  @Test
  public void testFromQuotedTableExpression() {
    for (String path : new String[] { "one.parq", "two three.parq", "four   five.parq", "relative/path/file.parq", "/absolute/path/item.parq" }) {
      String queryExpression = String.format("select * from '%s'", path);
      TextQueryLexer lexer = new TextQueryLexer(queryExpression, true);
      TextQueryTreeRootExpression root = new TextQueryTreeRootExpression(lexer);

      TextQueryVariableExpression tableExpression = root.asSelectStatement()
        .from()
        .tableSet()
        .tables()
        .first();

      assertTrue(tableExpression.is(TextQueryExpressionType.STRING));
      TextQueryStringExpression stringExpression = (TextQueryStringExpression)tableExpression;

      String tableName = stringExpression.asString();
      assertEquals(path, tableName);
    }
  }

  @Test
  public void testWhereExpression() {
    for (String lhs : new String[] { "one", "two.three", "four", "five.six", "6", "10" }) {
      for (String rhs : new String[] {"100", "four.five", "seven.eight.nine.ten", "some", "12", "10" }) {
        for (String operator : new String[]{ "=", "!=", "<", ">" }) {
          String statement = String.format("select * from something where %s %s %s", lhs, operator, rhs);
          TextQueryLexer lexer = new TextQueryLexer(statement, true);
          TextQueryTreeRootExpression rootExpression = new TextQueryTreeRootExpression(lexer);

          TextQuerySelectStatementExpression selectStatement = rootExpression.asSelectStatement();
          assertTrue(selectStatement.columnSet().containsWildcardColumn());

          assertWhereIsLike(selectStatement.where(), lhs, operator, rhs);
        }
      }
    }
  }

  public void assertWhereIsLike(TextQueryWhereExpression whereExpression, String lhs, String operator, String rhs) {
    TextQueryInfixExpression infixExpression = whereExpression.infixExpression();

    assertEquals(getLexedTypeForString(lhs), infixExpression.lhs().type());
    assertEquals(getLexedTypeForString(rhs), infixExpression.rhs().type());

    assertEquals(lhs, infixExpression.lhs().toString());
    assertEquals(operator, infixExpression.operator().toString());
    assertEquals(rhs, infixExpression.rhs().toString());
  }

  public TextQueryExpressionType getLexedTypeForString(String string) {
    if (Character.isAlphabetic(string.charAt(0))) return TextQueryExpressionType.NAMED_COLUMN;
    if (Character.isDigit(string.charAt(0))) return TextQueryExpressionType.NUMERIC;

    throw new NotImplementedException();
  }
}
