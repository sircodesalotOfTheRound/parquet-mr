package org.apache.parquet.parqour.query.expressions;

import junit.framework.Assert;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryStringExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQuerySelectStatementExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryWhereExpression;
import org.apache.parquet.parqour.query.expressions.variable.TextQueryUdfExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryColumnExpression;
import org.apache.parquet.parqour.query.expressions.variable.parenthetical.TextQueryParentheticalExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 7/24/15.
 */
public class TestInfixExpressions {
  @Test
  public void testVariableWhereExpression() {
    for (String lhs : new String[]{"one", "two.three", "four", "five.six", "6", "10"}) {
      for (String rhs : new String[]{"100", "four.five", "seven.eight.nine.ten", "some", "12", "10", "'string'"}) {
        for (String operator : new String[]{"=", "!=", "<", ">", "<=", ">="}) {
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

  private void assertWhereIsLike(TextQueryWhereExpression whereExpression, String lhs, String operator, String rhs) {
    TextQueryInfixExpression infixExpression = (TextQueryInfixExpression) whereExpression.predicate();

    assertEquals(getLexedTypeForString(lhs), infixExpression.lhs().type());
    assertEquals(getLexedTypeForString(rhs), infixExpression.rhs().type());

    String rhsWithQuotesRemoved = rhs.replace("'", "");

    assertEquals(lhs, infixExpression.lhs().toString());
    assertEquals(operator, infixExpression.operator().toString());
    assertEquals(rhsWithQuotesRemoved, infixExpression.rhs().toString());
  }

  public TextQueryExpressionType getLexedTypeForString(String string) {
    char firstCharacter = string.charAt(0);

    if (Character.isAlphabetic(firstCharacter)) {
      if (string.contains("(")) {
        return TextQueryExpressionType.UDF;
      } else {
        return TextQueryExpressionType.NAMED_COLUMN;
      }
    } else if (Character.isDigit(firstCharacter)) {
      return TextQueryExpressionType.NUMERIC;
    } else if (firstCharacter == '\'') {
      return TextQueryExpressionType.STRING;
    }

    throw new NotImplementedException();
  }

  @Test
  public void testNestedInfixExpression() {
    TextQueryTreeRootExpression statement = TextQueryTreeRootExpression.fromString("select * where 100 != 1000 and 'something' >= udf()");
    TextQuerySelectStatementExpression selectStatement = statement.asSelectStatement();
    TextQueryInfixExpression andExpression = (TextQueryInfixExpression) selectStatement.where().predicate();

    assertEquals("and", andExpression.operator().toString());

    TextQueryInfixExpression notEqualsExpression = (TextQueryInfixExpression) andExpression.lhs();
    assertEquals("!=", notEqualsExpression.operator().toString());
    assertEquals(100, (int) ((TextQueryNumericExpression) notEqualsExpression.lhs()).asInteger());
    assertEquals(1000, (int) ((TextQueryNumericExpression) notEqualsExpression.rhs()).asInteger());

    TextQueryInfixExpression greaterThanOrEqualsExpression = (TextQueryInfixExpression) andExpression.rhs();
    assertEquals(">=", greaterThanOrEqualsExpression.operator().toString());
    assertEquals("something", ((TextQueryStringExpression) greaterThanOrEqualsExpression.lhs()).asString());

    assertEquals("udf", ((TextQueryUdfExpression) greaterThanOrEqualsExpression.rhs()).functionName().toString());
    assertEquals(0, ((TextQueryUdfExpression) greaterThanOrEqualsExpression.rhs()).parameterCount());
  }

  @Test
  public void testComplexInfixExpression() {
    String statement = "select * from something where "
      + "left_function(first_inner(a.column) + second_inner(42 - 69), third_inner(x.y, y.x)) <= 'string value'";

    TextQueryLexer lexer = new TextQueryLexer(statement, true);
    TextQueryTreeRootExpression rootExpression = new TextQueryTreeRootExpression(lexer);

    TextQuerySelectStatementExpression selectStatement = rootExpression.asSelectStatement();
    TextQueryInfixExpression lessThanExpression = (TextQueryInfixExpression) selectStatement.where().predicate();

    TextQueryUdfExpression leftFunctionExpression = (TextQueryUdfExpression) lessThanExpression.lhs();
    TextQueryStringExpression stringValueExpression = (TextQueryStringExpression) lessThanExpression.rhs();

    assertEquals(TextQueryExpressionType.UDF, lessThanExpression.lhs().type());
    assertEquals(2, leftFunctionExpression.parameterCount());
    assertEquals("left_function", leftFunctionExpression.functionName().toString());

    assertEquals("<=", lessThanExpression.operator().toString());

    assertEquals(TextQueryExpressionType.STRING, lessThanExpression.rhs().type());
    assertEquals("string value", stringValueExpression.asString());

    TextQueryInfixExpression additionInnerExpression = leftFunctionExpression.parameters().firstAs(TextQueryInfixExpression.class);
    assertEquals("+", additionInnerExpression.operator().toString());

    TextQueryUdfExpression firstInner = (TextQueryUdfExpression) additionInnerExpression.lhs();
    assertEquals("first_inner", firstInner.functionName().toString());
    assertEquals(1, firstInner.parameterCount());

    assertEquals("a.column", firstInner.parameters().firstAs(TextQueryColumnExpression.class).toString());

    TextQueryUdfExpression secondInner = (TextQueryUdfExpression) additionInnerExpression.rhs();
    assertEquals("second_inner", secondInner.functionName().toString());
    assertEquals(1, firstInner.parameterCount());

    TextQueryInfixExpression subtractionExpression = secondInner.parameters().firstAs(TextQueryInfixExpression.class);
    TextQueryNumericExpression lhsOneValue = (TextQueryNumericExpression)subtractionExpression.lhs();
    TextQueryNumericExpression rhsTwoValue = (TextQueryNumericExpression)subtractionExpression.rhs();
    assertEquals(42, (int)lhsOneValue.asInteger());
    assertEquals("-", subtractionExpression.operator().toString());
    assertEquals(69, (int)rhsTwoValue.asInteger());

    TextQueryUdfExpression thirdInner = leftFunctionExpression.parameters().secondAs(TextQueryUdfExpression.class);

    assertEquals(2, thirdInner.parameterCount());

    TextQueryColumnExpression xDotYColumn = thirdInner.parameters().firstAs(TextQueryColumnExpression.class);
    TextQueryColumnExpression yDotXColumn = thirdInner.parameters().secondAs(TextQueryColumnExpression.class);
    assertEquals("x.y", xDotYColumn.toString());
    assertEquals("y.x", yDotXColumn.toString());
  }

  @Test
  public void testOperatorPrecedence() {
    assertEquals(14, computeFromSelectStatement("select 1 * 2 + 3 * 4"));
    assertEquals(21, computeFromSelectStatement("select (1 + 2) * (3 + 4)"));
    assertEquals(10, computeFromSelectStatement("select 1 + 2 + 3 + 4"));
    assertEquals(6, computeFromSelectStatement("select (18 / 6) * (3 + 4 - 5)"));
    assertEquals(120 % 7, computeFromSelectStatement("select (1 * 2 * 3 * 4 * 5) % 7"));

    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString("select * where (5 != 6 and 7 >= 8) or (true())");
    TextQueryInfixExpression orExpression = (TextQueryInfixExpression) rootExpression.asSelectStatement().where().predicate();

    assertEquals(InfixOperator.OR, orExpression.operator());

    TextQueryParentheticalExpression lhsParentheticalExpression = (TextQueryParentheticalExpression) orExpression.lhs();
    TextQueryInfixExpression andExpression = (TextQueryInfixExpression) lhsParentheticalExpression.innerExpression();

    assertEquals(InfixOperator.AND, andExpression.operator());
    TextQueryInfixExpression fiveNotEqualsSix = (TextQueryInfixExpression) andExpression.lhs();
    assertEquals(InfixOperator.NOT_EQUALS, fiveNotEqualsSix.operator());
    assertEquals(5, (int) ((TextQueryNumericExpression) fiveNotEqualsSix.lhs()).asInteger());
    assertEquals(6, (int) ((TextQueryNumericExpression) fiveNotEqualsSix.rhs()).asInteger());

    TextQueryInfixExpression sevenGtOrEqToEight = (TextQueryInfixExpression) andExpression.rhs();
    assertEquals(InfixOperator.GREATER_THAN_OR_EQUALS, sevenGtOrEqToEight.operator());
    assertEquals(7, (int)((TextQueryNumericExpression)sevenGtOrEqToEight.lhs()).asInteger());
    assertEquals(8, (int)((TextQueryNumericExpression)sevenGtOrEqToEight.rhs()).asInteger());

    TextQueryParentheticalExpression rhsParentheticalExpression = (TextQueryParentheticalExpression) orExpression.rhs();
    TextQueryUdfExpression trueExpression = (TextQueryUdfExpression) rhsParentheticalExpression.innerExpression();

    assertEquals("true", trueExpression.functionName().toString());
    assertEquals(0, trueExpression.parameterCount());
  }

  public int computeFromSelectStatement(String selectStatementExpression) {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString(selectStatementExpression);
    TextQueryInfixExpression textQueryInfixExpression = rootExpression.asSelectStatement()
      .columnSet()
      .columns()
      .firstAs(TextQueryInfixExpression.class);

    return compute(textQueryInfixExpression);
  }

  public int compute(TextQueryExpression expression) {
    if (expression instanceof TextQueryNumericExpression) {
      return ((TextQueryNumericExpression) expression).asInteger();
    } else if (expression instanceof TextQueryParentheticalExpression) {
      return compute(((TextQueryParentheticalExpression) expression).innerExpression());
    } else if (expression instanceof TextQueryInfixExpression) {
      int lhs = compute(((TextQueryInfixExpression) expression).lhs());
      int rhs = compute(((TextQueryInfixExpression) expression).rhs());

      switch (((TextQueryInfixExpression) expression).operator()) {
        case PLUS: return lhs + rhs;
        case MINUS: return lhs - rhs;
        case MULTIPLY: return lhs * rhs;
        case DIVIDE: return lhs / rhs;
        case MODULO: return lhs % rhs;
      }
    }

    throw new NotImplementedException();
  }
}
