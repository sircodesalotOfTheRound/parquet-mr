package org.apache.parquet.parqour.query.expressions.predicate;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.testable.*;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sircodesalot on 7/27/15.
 */
public abstract class TextQueryTestablePredicateExpression extends TextQueryVariableExpression {
  private static Set<InfixOperator> testableOperators = new HashSet<InfixOperator>() {{
    add(InfixOperator.EQUALS);
    add(InfixOperator.NOT_EQUALS);
    add(InfixOperator.LESS_THAN);
    add(InfixOperator.LESS_THAN_OR_EQUALS);
    add(InfixOperator.GREATER_THAN);
    add(InfixOperator.GREATER_THAN_OR_EQUALS);
    add(InfixOperator.MATCHES);
    add(InfixOperator.IS);
  }};

  public TextQueryTestablePredicateExpression(TextQueryExpression parent, TextQueryExpressionType type) {
    super(parent, type);
  }

  public abstract boolean test();

  public static TextQueryTestablePredicateExpression fromExpression(TextQueryVariableExpression expression) {
    if (expression.is(TextQueryExpressionType.INFIX)) {
      TextQueryInfixExpression infixExpression = (TextQueryInfixExpression)expression;
      switch (infixExpression.operator()) {
        case EQUALS:
          return new TextQueryTestableEqualsExpression(infixExpression);
        case NOT_EQUALS:
          return new TextQueryTestableNotEqualsExpression(infixExpression);

        case LESS_THAN:
          return new TextQueryTestableLessThanExpression(infixExpression);
        case LESS_THAN_OR_EQUALS:
          return new TextQueryTestableLessThanOrEqualsExpression(infixExpression);

        case GREATER_THAN:
          return new TextQueryTestableGreaterThanExpression(infixExpression);
        case GREATER_THAN_OR_EQUALS:
          return new TextQueryTestableGreaterThanOrEqualsExpression(infixExpression);

        case MATCHES:
          return new TextQueryMatchesExpression(infixExpression);
        case IS:
          return new TextQueryIsExpression(infixExpression);
      }
    }

    throw new NotImplementedException();
  }

  public static boolean isTestablePredicateExpression(TextQueryInfixExpression expression) {
    return testableOperators.contains(expression.operator());
  }
}
