package org.apache.parquet.parqour.query.expressions.variable.infix;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class TextQueryInfixExpression extends TextQueryVariableExpression {
  private TextQueryVariableExpression lhs;
  private TextQueryVariableExpression rhs;
  private InfixOperator operator;

  private TextQueryInfixExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.INFIX);

    this.lhs = readLhs(lexer);
    this.operator = readOperationToken(lexer);
    this.rhs = readRhs(lexer);
  }

  private TextQueryVariableExpression readLhs(TextQueryLexer lexer) {
    return TextQueryVariableExpression.readIgnoringInfixExpressions(this, lexer);
  }

  private InfixOperator readOperationToken(TextQueryLexer lexer) {
    return InfixOperator.readInfixOperator(lexer);
  }

  private TextQueryVariableExpression readRhs(TextQueryLexer lexer) {
    lexer.setUndoPoint();
    TextQueryVariableExpression expression = TextQueryVariableExpression.readIgnoringInfixExpressions(this, lexer);

    // If reading the last expression ended in a math operator, then
    if (InfixOperator.isInfixToken(lexer)) {
      lexer.rollbackToUndoPoint();
      return TextQueryInfixExpression.read(this, lexer);
    } else {
      lexer.clearUndoPoint();
      return expression;
    }
  }

  // Re-structure the tree to reflect proper operator precedence.
  public static TextQueryInfixExpression updateOperatorPrecedence(TextQueryInfixExpression expression) {
    if (expression.lhs() instanceof TextQueryInfixExpression) {
      updateOperatorPrecedence((TextQueryInfixExpression) expression.lhs);
    }

    if (expression.rhs() instanceof TextQueryInfixExpression) {
      updateOperatorPrecedence((TextQueryInfixExpression) expression.rhs);
    }

    if (expression.hasHigherPrecedenceThan(expression.lhs)) {
      expression = rotateLeft(expression);
    }

    if (expression.hasHigherPrecedenceThan(expression.rhs)) {
      expression = rotateRight(expression);
    }

    return expression;
  }

  public boolean hasHigherPrecedenceThan(TextQueryExpression rhs) {
    if (rhs != null && rhs instanceof TextQueryInfixExpression) {
      return this.operator.hasHigherPrecedenceThan(((TextQueryInfixExpression) rhs).operator);
    } else {
      return false;
    }
  }

  // Swap parents on these mathematical nodes.
  public static TextQueryInfixExpression rotateLeft(TextQueryInfixExpression expression) {
    TextQueryInfixExpression newParent = (TextQueryInfixExpression)expression.lhs;
    expression.lhs = newParent.rhs;

    if (newParent.rhs != null) {
      newParent.rhs.setParent(expression);
    }

    newParent.rhs = expression;
    expression.setParent(newParent);

    return newParent;
  }

  // Swap parents on these mathematical nodes.
  public static TextQueryInfixExpression rotateRight(TextQueryInfixExpression expression) {
    TextQueryInfixExpression newParent = (TextQueryInfixExpression)expression.rhs;
    expression.rhs = newParent.lhs;

    if (newParent.lhs != null) {
      newParent.lhs.setParent(expression);
    }

    newParent.lhs = expression;
    expression.setParent(newParent);

    return newParent;
  }

  public TextQueryVariableExpression lhs() {
    return this.lhs;
  }

  public TextQueryVariableExpression rhs() {
    return this.rhs;
  }

  public InfixOperator operator() {
    return this.operator;
  }

  public static TextQueryInfixExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    TextQueryInfixExpression infixExpression = new TextQueryInfixExpression(parent, lexer);
    return updateOperatorPrecedence(infixExpression);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    if (InfixExpressionCalculator.canPrecomputeExpression(this)) {
      return InfixExpressionCalculator.precomputeExpression(this);
    } else if (TextQueryTestablePredicateExpression.isTestablePredicateExpression(this)) {
      return TextQueryTestablePredicateExpression.fromExpression(this);
    }

    throw new NotImplementedException();
  }


  @Override
  public TextQueryVariableExpression negate() {
    return this.simplify(this.parent()).negate();
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    if (lhs != null) {
      lhs.accept(visitor);
    }

    if (rhs != null) {
      rhs.accept(visitor);
    }

    return null;
  }

}
