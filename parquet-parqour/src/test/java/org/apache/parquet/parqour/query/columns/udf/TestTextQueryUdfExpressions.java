package org.apache.parquet.parqour.query.columns.udf;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQuerySelectStatementExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.variable.TextQueryUdfExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryWildcardExpression;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 7/24/15.
 */
public class TestTextQueryUdfExpressions {
  @Test
  public void testQuerySimpleUdfExpression() {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString("select count(*)");
    TextQuerySelectStatementExpression selectExpression = rootExpression.asSelectStatement();

    TextQueryUdfExpression udfExpression = selectExpression.columnSet().columns()
      .map(new Projection<TextQueryVariableExpression, TextQueryUdfExpression>() {
        @Override
        public TextQueryUdfExpression apply(TextQueryVariableExpression variableExpression) {
          return (TextQueryUdfExpression)variableExpression;
        }
      }).first();

    assertEquals("count", udfExpression.functionName().toString());
    assertEquals(1, udfExpression.parameterCount());
    assertTrue(udfExpression.parameters().first() instanceof TextQueryWildcardExpression);
  }

  @Test
  public void testZeroParameterUdfExpression() {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString("select random()");
    TextQuerySelectStatementExpression selectExpression = rootExpression.asSelectStatement();

    TextQueryUdfExpression udfExpression = selectExpression.columnSet().columns()
      .map(new Projection<TextQueryVariableExpression, TextQueryUdfExpression>() {
        @Override
        public TextQueryUdfExpression apply(TextQueryVariableExpression variableExpression) {
          return (TextQueryUdfExpression)variableExpression;
        }
      }).first();

    assertEquals("random", udfExpression.functionName().toString());
    assertEquals(0, udfExpression.parameterCount());
  }


  @Test
  public void testSingleColumnParameterUdfExpression() {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString("select length(some.column)");
    TextQuerySelectStatementExpression selectExpression = rootExpression.asSelectStatement();

    TextQueryUdfExpression udfExpression = selectExpression.columnSet().columns()
      .map(new Projection<TextQueryVariableExpression, TextQueryUdfExpression>() {
        @Override
        public TextQueryUdfExpression apply(TextQueryVariableExpression variableExpression) {
          return (TextQueryUdfExpression)variableExpression;
        }
      }).first();

    assertEquals("length", udfExpression.functionName().toString());
    assertEquals(1, udfExpression.parameterCount());
    assertTrue(udfExpression.parameters().first() instanceof TextQueryNamedColumnExpression);

    TextQueryNamedColumnExpression column = udfExpression.parameters().firstAs(TextQueryNamedColumnExpression.class);
    assertEquals("some.column", column.path());
  }


  @Test
  public void testTwoColumnUdfExpression() {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString("select regex(person.email, '.*?@google.com$')");
    TextQuerySelectStatementExpression selectExpression = rootExpression.asSelectStatement();

    TextQueryUdfExpression udfExpression = selectExpression.columnSet().columns()
      .map(new Projection<TextQueryVariableExpression, TextQueryUdfExpression>() {
        @Override
        public TextQueryUdfExpression apply(TextQueryVariableExpression variableExpression) {
          return (TextQueryUdfExpression)variableExpression;
        }
      }).first();

    assertEquals("length", udfExpression.functionName().toString());
    assertEquals(2, udfExpression.parameterCount());
    assertTrue(udfExpression.parameters().first() instanceof TextQueryNamedColumnExpression);

    TextQueryNamedColumnExpression column = udfExpression.parameters().firstAs(TextQueryNamedColumnExpression.class);
    assertEquals("person.email", column.path());


  }
}
