package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.constant.TextQueryStringExpression;
import org.apache.parquet.parqour.query.expressions.variable.constant.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQuerySelectStatementExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.expressions.variable.parenthetical.TextQueryParentheticalExpression;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 7/26/15.
 */
public class TestAliases {
  @Test
  public void testAlias() {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression
      .fromString("select (2 + 3) as five, 'something' as string, column as alias");

    TextQuerySelectStatementExpression selectStatement = rootExpression.asSelectStatement();
    TransformCollection<TextQueryVariableExpression> columns = selectStatement.columnSet().columns();

    TextQueryInfixExpression asFiveAlias = columns.firstAs(TextQueryInfixExpression.class);
    assertEquals("five", ((TextQueryNamedColumnExpression) asFiveAlias.rhs()).identifier().toString());

    TextQueryParentheticalExpression parentheticalExpression = (TextQueryParentheticalExpression) asFiveAlias.lhs();
    TextQueryInfixExpression twoPlusThree = (TextQueryInfixExpression) parentheticalExpression.innerExpression();
    assertEquals(InfixOperator.PLUS, twoPlusThree.operator());
    assertEquals(2, (int) ((TextQueryNumericExpression) twoPlusThree.lhs()).asInteger());
    assertEquals(3, (int) ((TextQueryNumericExpression) twoPlusThree.rhs()).asInteger());

    TextQueryInfixExpression asStringAlias = columns.secondAs(TextQueryInfixExpression.class);
    assertEquals("something", ((TextQueryStringExpression) asStringAlias.lhs()).asString());
    assertEquals("string", ((TextQueryNamedColumnExpression) asStringAlias.rhs()).identifier().toString());

    TextQueryInfixExpression asAliasAlias = (TextQueryInfixExpression) columns.get(2);
    assertEquals("column", ((TextQueryNamedColumnExpression) asAliasAlias.lhs()).identifier().toString());
    assertEquals("alias", ((TextQueryNamedColumnExpression) asAliasAlias.rhs()).identifier().toString());
  }
}
