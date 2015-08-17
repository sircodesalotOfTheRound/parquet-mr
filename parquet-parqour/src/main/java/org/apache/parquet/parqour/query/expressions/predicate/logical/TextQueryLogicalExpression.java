package org.apache.parquet.parqour.query.expressions.predicate.logical;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sircodesalot on 7/27/15.
 */
public abstract class TextQueryLogicalExpression extends TextQueryVariableExpression {
  private static Set<InfixOperator> logicalOperators = new HashSet<InfixOperator>() {{
    add(InfixOperator.AND);
    add(InfixOperator.OR);
  }};

  private final TextQueryVariableExpression lhs;
  private final TextQueryVariableExpression rhs;

  public TextQueryLogicalExpression(TextQueryVariableExpression lhs, TextQueryVariableExpression rhs, TextQueryExpressionType type) {
    super(lhs.parent(), type);

    this.lhs = lhs;
    this.rhs = rhs;
  }

  public TextQueryVariableExpression lhs() { return this.lhs; }
  public abstract InfixOperator operator();
  public TextQueryVariableExpression rhs() { return this.rhs; }

  public static boolean isLogicalExpression(TextQueryInfixExpression infixExpression) {
    return logicalOperators.contains(infixExpression.operator());
  }

  public static TextQueryVariableExpression fromExpression(TextQueryInfixExpression expression) {
    if (expression.is(TextQueryExpressionType.INFIX)) {
      switch (expression.operator()) {
        case AND:
          return new TextQueryLogicalAndExpression(expression.lhs(), expression.rhs());
        case OR:
          return new TextQueryLogicalOrExpression(expression.lhs(), expression.rhs());
      }
    }

    throw new NotImplementedException();
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return null;
  }
}
