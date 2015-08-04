package org.apache.parquet.parqour.query.expressions.predicate.testable;

import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.cursor.implementations.noniterable.constant.ConstantValueCursor;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;

/**
 * Created by sircodesalot on 7/27/15.
 */
public abstract class TextQueryTestableBinaryExpression extends TextQueryTestablePredicateExpression {
  protected final TextQueryInfixExpression infixExpression;
  protected final Cursor lhsCursor;
  protected final Cursor rhsCursor;

  protected final boolean lhsIsCached;
  protected final boolean rhsIsCached;

  protected Comparable lastSeenLhs;
  protected Comparable lastSeenRhs;

  public TextQueryTestableBinaryExpression(TextQueryInfixExpression infixExpression, TextQueryExpressionType type) {
    super(infixExpression.parent(), type);

    this.infixExpression = infixExpression;
    this.lhsCursor = infixExpression.lhs().getCursor();
    this.rhsCursor = infixExpression.rhs().getCursor();

    this.lastSeenLhs = attemptToCache(lhsCursor);
    this.lastSeenRhs = attemptToCache(rhsCursor);

    this.lhsIsCached = (lastSeenLhs != null);
    this.rhsIsCached = (lastSeenRhs != null);
  }

  public Comparable attemptToCache(Cursor cursor) {
    if (cursor instanceof ConstantValueCursor) {
      return (Comparable)cursor.value();
    } else {
      return null;
    }
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return infixExpression.simplify(parent);
  }


  public TextQueryVariableExpression lhs() { return infixExpression.lhs(); }
  public abstract InfixOperator operator();
  public TextQueryVariableExpression rhs() { return infixExpression.rhs(); }
}
