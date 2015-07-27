package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryStringExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.variable.TextQueryUdfExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryColumnSetExpression;
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
  }

  private TextQueryVariableExpression simplifiedFirstColumn(String selectStatement) {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString(selectStatement);
    TextQueryColumnSetExpression columns = rootExpression.asSelectStatement().columnSet();
    return columns.columns().first().simplify(columns);
  }
}
