package org.apache.parquet.parqour.query.expressions.predicate.testable;

import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.cursor.implementations.noniterable.resolved.ConstantValueCursor;
import org.apache.parquet.parqour.cursor.implementations.noniterable.resolved.EvaluatedValueCursor;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 7/27/15.
 */
public abstract class TextQueryTestableBinaryExpression<T> extends TextQueryTestablePredicateExpression {
  protected final TextQueryInfixExpression infixExpression;
  protected final Cursor lhsCursor;
  protected final Cursor rhsCursor;

  protected final boolean lhsIsCached;
  protected final boolean rhsIsCached;

  protected T lastSeenLhs;
  protected T lastSeenRhs;

  public TextQueryTestableBinaryExpression(TextQueryInfixExpression infixExpression, TextQueryExpressionType type) {
    super(infixExpression.parent(), type);

    this.infixExpression = infixExpression;
    // TODO: Rampant simplifications.
    this.lhsCursor = infixExpression.lhs().simplify(this).getCursor();
    this.rhsCursor = infixExpression.rhs().simplify(this).getCursor();

    this.lastSeenLhs = attemptToCache(lhsCursor);
    this.lastSeenRhs = attemptToCache(rhsCursor);

    this.lhsIsCached = (lastSeenLhs != null);
    this.rhsIsCached = (lastSeenRhs != null);
  }

  public T attemptToCache(Cursor cursor) {
    if (cursor instanceof ConstantValueCursor) {
      return (T)cursor.value();
    } else {
      return null;
    }
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return infixExpression.simplify(parent);
  }

  @Override
  public void bindToTree(IngestTree tree) {
    lhs().bindToTree(tree);
    rhs().bindToTree(tree);
  }

  @Override
  public EvaluationDifficulty evaluationDifficulty() {
    return EvaluationDifficulty.max(lhs().evaluationDifficulty(), rhs().evaluationDifficulty());
  }

  @Override
  public Cursor getCursor() {
    // Todo: give this a better name.
    return new EvaluatedValueCursor("evaluated-cursor", -1) {
      @Override
      public Object value() {
        return test();
      }
    };
  }

  public TextQueryVariableExpression lhs() { return infixExpression.lhs(); }
  public abstract InfixOperator operator();
  public TextQueryVariableExpression rhs() { return infixExpression.rhs(); }



  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return null;
  }
}
