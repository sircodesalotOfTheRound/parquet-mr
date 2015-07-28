package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.logical.TextQueryLogicalAndExpression;
import org.apache.parquet.parqour.query.expressions.predicate.logical.TextQueryLogicalOrExpression;
import org.apache.parquet.parqour.query.expressions.predicate.testable.*;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryStringExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.variable.TextQueryUdfExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryColumnSetExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class TestNegation {
  @Test
  public void testComparableSimplification() {
    TextQueryTestableNotEqualsExpression oneNotEqualsTen = (TextQueryTestableNotEqualsExpression) simplifiedFirstColumn("select not (1 = 10)");
    assertEquals(InfixOperator.NOT_EQUALS, oneNotEqualsTen.operator());

    TextQueryTestableEqualsExpression oneEqualsTen = (TextQueryTestableEqualsExpression) simplifiedFirstColumn("select not (1 != 10)");
    assertEquals(InfixOperator.EQUALS, oneEqualsTen.operator());

    TextQueryTestableGreaterThanOrEqualsExpression oneIsGreaterThanOrEqualToTen = (TextQueryTestableGreaterThanOrEqualsExpression) simplifiedFirstColumn("select not (1 < 10)");
    assertEquals(InfixOperator.GREATER_THAN_OR_EQUALS, oneIsGreaterThanOrEqualToTen .operator());

    TextQueryTestableGreaterThanExpression oneIsGreaterThanTen = (TextQueryTestableGreaterThanExpression) simplifiedFirstColumn("select not (1 <= 10)");
    assertEquals(InfixOperator.GREATER_THAN, oneIsGreaterThanTen.operator());

    TextQueryTestableLessThanExpression oneIsLessThanTen = (TextQueryTestableLessThanExpression) simplifiedFirstColumn("select not (1 >= 10)");
    assertEquals(InfixOperator.LESS_THAN, oneIsLessThanTen.operator());

    TextQueryTestableLessThanOrEqualsExpression oneIsLessThanOrEqualToTen = (TextQueryTestableLessThanOrEqualsExpression) simplifiedFirstColumn("select not (1 > 10)");
    assertEquals(InfixOperator.LESS_THAN_OR_EQUALS, oneIsLessThanOrEqualToTen.operator());

    TextQueryLogicalOrExpression orExpression = (TextQueryLogicalOrExpression) simplifiedFirstColumn("select not (first() and second())");
    assertEquals(InfixOperator.OR, orExpression.operator());
    assertTrue(((TextQueryUdfExpression) orExpression.lhs()).isNegated());
    assertTrue(((TextQueryUdfExpression) orExpression.rhs()).isNegated());

    TextQueryLogicalAndExpression andExpression = (TextQueryLogicalAndExpression) simplifiedFirstColumn("select not (first() or second())");
    assertEquals(InfixOperator.AND, andExpression.operator());
    assertTrue(((TextQueryUdfExpression) andExpression.lhs()).isNegated());
    assertTrue(((TextQueryUdfExpression) andExpression.rhs()).isNegated());
  }

  private TextQueryVariableExpression simplifiedFirstColumn(String selectStatement) {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString(selectStatement);
    TextQueryColumnSetExpression columns = rootExpression.asSelectStatement().columnSet();
    return columns.columns().first().simplify(columns);
  }
}
