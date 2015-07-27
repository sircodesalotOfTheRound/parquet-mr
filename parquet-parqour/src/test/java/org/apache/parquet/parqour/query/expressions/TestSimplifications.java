package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryStringExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
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
  }

  @Test
  public void testParenthasisElision() {
    TextQueryNumericExpression fourtyTwo = (TextQueryNumericExpression) simplifiedFirstColumn("select ((((42))))");
    assertEquals(42, (int) fourtyTwo.asInteger());
  }

  private TextQueryVariableExpression simplifiedFirstColumn(String selectStatement) {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString(selectStatement);
    TextQueryColumnSetExpression columns = rootExpression.asSelectStatement().columnSet();
    return columns.columns().first().simplify(columns);
  }
}
