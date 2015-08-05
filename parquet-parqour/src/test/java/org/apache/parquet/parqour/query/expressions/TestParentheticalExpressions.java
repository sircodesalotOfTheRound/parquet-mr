package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.constant.TextQueryStringExpression;
import org.apache.parquet.parqour.query.expressions.variable.constant.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQuerySelectStatementExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.variable.TextQueryUdfExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.expressions.variable.parenthetical.TextQueryParentheticalExpression;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 7/24/15.
 */
public class TestParentheticalExpressions {
  @Test
  public void testNestedInfixExpression() {
    TextQueryTreeRootExpression statement = TextQueryTreeRootExpression.fromString("select (3 + 5), (udf()), ('string')");
    TextQuerySelectStatementExpression selectStatement = statement.asSelectStatement();

    boolean allEntriesHaveParenthasis = selectStatement.columnSet().columns()
      .all(new Predicate<TextQueryVariableExpression>() {
        @Override
        public boolean test(TextQueryVariableExpression expression) {
          return expression instanceof TextQueryParentheticalExpression;
        }
      });

    assertTrue(allEntriesHaveParenthasis);

    TextQueryCollection<TextQueryParentheticalExpression> parentheticalExpressions = selectStatement
      .columnSet()
      .columns()
      .map(new Projection<TextQueryVariableExpression, TextQueryParentheticalExpression>() {
        @Override
        public TextQueryParentheticalExpression apply(TextQueryVariableExpression expression) {
          return (TextQueryParentheticalExpression) expression;
        }
      });

    TextQueryInfixExpression threePlusFive = (TextQueryInfixExpression) parentheticalExpressions.get(0).innerExpression();
    assertEquals(3, (int) ((TextQueryNumericExpression) threePlusFive.lhs()).asInteger());
    assertEquals("+", threePlusFive.operator().toString());
    assertEquals(5, (int) ((TextQueryNumericExpression) threePlusFive.rhs()).asInteger());

    TextQueryUdfExpression udf = (TextQueryUdfExpression) parentheticalExpressions.get(1).innerExpression();
    assertEquals("udf", udf.functionName().toString());
    assertEquals(0, udf.parameterCount());

    TextQueryStringExpression string = (TextQueryStringExpression) parentheticalExpressions.get(2).innerExpression();
    assertEquals("string", string.asString());

  }
}
