package org.apache.parquet.parqour.query.expressions.variable.infix;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.constant.TextQueryStringExpression;
import org.apache.parquet.parqour.query.expressions.variable.constant.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.tokens.TextQueryNumericToken;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class InfixExpressionCalculator {
  private static Set<InfixOperator> computableOperators = new HashSet<InfixOperator>() {{
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
    Object lhs = getValue(expression.lhs().simplify(expression));
    Object rhs = getValue(expression.rhs().simplify(expression));

    if (lhs instanceof String || rhs instanceof String) {
      return new TextQueryStringExpression(expression.parent(), precomputeString(lhs, rhs));
    } else {
      BigInteger result = precomputeNumeric((Integer) lhs, expression.operator(), (Integer) rhs);
      return new TextQueryNumericExpression(expression.parent(), new TextQueryNumericToken(result));
    }
  }

  private static String precomputeString(Object lhs, Object rhs) {
    return String.format("%s%s", lhs, rhs);
  }

  public static BigInteger precomputeNumeric(Integer lhs, InfixOperator operator,  Integer rhs) {
    BigInteger result = null;
    switch (operator) {
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

    return result;
  }

  public static Object getValue(TextQueryVariableExpression expression) {
    if (expression.is(TextQueryExpressionType.NUMERIC)) {
      return ((TextQueryNumericExpression)expression).asInteger();
    } else if (expression.is(TextQueryExpressionType.STRING)){
      return ((TextQueryStringExpression)expression).asString();
    }

    throw new NotImplementedException();
  }
}
