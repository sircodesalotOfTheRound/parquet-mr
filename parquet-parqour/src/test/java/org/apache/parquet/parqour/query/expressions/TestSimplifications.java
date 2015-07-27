package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.testable.TextQueryTestableEqualsExpression;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryStringExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.variable.TextQueryUdfExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryColumnSetExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class TestSimplifications {
  @Test
  public void testIdempotent() {
    TextQueryNumericExpression oneHundred = (TextQueryNumericExpression) simplifiedFirstColumn("select 100");
    assertEquals(100, (int) oneHundred.asInteger());

    TextQueryStringExpression something = (TextQueryStringExpression) simplifiedFirstColumn("select 'something'");
    assertEquals("something", something.asString());

    TextQueryUdfExpression udf = (TextQueryUdfExpression) simplifiedFirstColumn("select udf()");
    assertEquals("udf", udf.functionName().toString());
    assertEquals(0, udf.parameterCount());
  }

  @Test
  public void testParenthasisElision() {
    TextQueryNumericExpression fourtyTwo = (TextQueryNumericExpression) simplifiedFirstColumn("select ((((42))))");
    assertEquals(42, (int) fourtyTwo.asInteger());
  }

  @Test
  public void testPrecomputation() {
    TextQueryNumericExpression ten = (TextQueryNumericExpression) simplifiedFirstColumn("select (5 + 5)");
    assertEquals(10, (int) ten.asInteger());

    TextQueryNumericExpression fiftyFour = (TextQueryNumericExpression) simplifiedFirstColumn("select (1 + 2 + 3)  * (4 + 5)");
    assertEquals(54, (int) fiftyFour.asInteger());

    TextQueryNumericExpression thirtyThree = (TextQueryNumericExpression) simplifiedFirstColumn("select 1 + 2 * 3 + 4 * 5 + 6");
    assertEquals(33, (int) thirtyThree.asInteger());

    TextQueryStringExpression oneTwoThreeSomething = (TextQueryStringExpression) simplifiedFirstColumn("select (123 + 'something')");
    assertEquals("123something", oneTwoThreeSomething.asString());

    TextQueryStringExpression somethingOneTwoThree = (TextQueryStringExpression) simplifiedFirstColumn("select ('something' + 123)");
    assertEquals("something123", somethingOneTwoThree .asString());
  }


  @Test
  public void testUdfExpressions() {
    TextQueryUdfExpression function = (TextQueryUdfExpression) simplifiedFirstColumn("select function((5 + 5) * 10)");
    assertEquals("function", function.functionName().toString());
    assertEquals(1, function.parameterCount());

    TextQueryNumericExpression oneHundred = function.parameters().firstAs(TextQueryNumericExpression.class);
    assertEquals(100, (int) oneHundred.asInteger());
  }

  private TextQueryVariableExpression simplifiedFirstColumn(String selectStatement) {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString(selectStatement);
    TextQueryColumnSetExpression columns = rootExpression.asSelectStatement().columnSet();
    return columns.columns().first().simplify(columns);
  }
}
