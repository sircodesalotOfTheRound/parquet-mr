package org.apache.parquet.parqour.query.expressions.variable.infix;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.tokens.TextQueryNumericToken;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class InfixExpressionCalculator {
  public static Set<InfixOperator> computableOperators = new HashSet<InfixOperator>() {{
    add(InfixOperator.PLUS);
    add(InfixOperator.MINUS);
    add(InfixOperator.MULTIPLY);
    add(InfixOperator.DIVIDE);
    add(InfixOperator.MODULO);
  }};


  public static boolean canPrecomputeExpression(TextQueryInfixExpression expression) {
    return (computableOperators.contains(expression.operator()) &&
      isComputableExpression(expression.lhs()) && isComputableExpression(expression.rhs()));
  }

  private static boolean isComputableExpression(TextQueryVariableExpression expression) {
    TextQueryVariableExpression simplifiedExpression = expression.simplify(expression);

    return (simplifiedExpression.is(TextQueryExpressionType.NUMERIC)
      || simplifiedExpression.is(TextQueryExpressionType.STRING));
  }

  public static TextQueryVariableExpression precomputeExpression(TextQueryInfixExpression expression) {
    Integer lhs = getValue(expression.lhs().simplify(expression));
    Integer rhs = getValue(expression.rhs().simplify(expression));

    BigInteger result = null;

    switch (expression.operator()) {
      case PLUS:
        result = BigInteger.valueOf(lhs + rhs);
        break;
      case MINUS:
        result = BigInteger.valueOf(lhs - rhs);
        break;
      case MULTIPLY:
        result = BigInteger.valueOf(lhs * rhs);
        break;
      case DIVIDE:
        result = BigInteger.valueOf(lhs / rhs);
        break;
      case MODULO:
        result = BigInteger.valueOf(lhs % rhs);
        break;

      default:
        throw new NotImplementedException();
    }

    return new TextQueryNumericExpression(expression.parent(), new TextQueryNumericToken(result));
  }

  public static Integer getValue(TextQueryVariableExpression expression) {
    if (expression.is(TextQueryExpressionType.NUMERIC)) {
      return ((TextQueryNumericExpression)expression).asInteger();
    } else if (expression.is(TextQueryExpressionType.STRING)){
      throw new NotImplementedException();
      //return ((TextQueryStringExpression)expression).asString();
    }

    throw new NotImplementedException();
  }
}
