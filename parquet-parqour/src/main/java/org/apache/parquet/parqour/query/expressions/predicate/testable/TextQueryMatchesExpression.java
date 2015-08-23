package org.apache.parquet.parqour.query.expressions.predicate.testable;

import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.TraversalInfo;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

import java.util.regex.Pattern;

/**
 * Created by sircodesalot on 8/6/15.
 */
public class TextQueryMatchesExpression extends TextQueryTestableBinaryExpression<String> {
  private final Pattern pattern;
  private boolean isNegated = false;

  public TextQueryMatchesExpression(TextQueryInfixExpression infixExpression) {
    super(infixExpression, TextQueryExpressionType.MATCHES);

    this.pattern = Pattern.compile(lastSeenRhs);
  }

  @Override
  public InfixOperator operator() {
    return InfixOperator.MATCHES;
  }

  @Override
  public boolean test() {
    if (!lhsIsCached) {
      lastSeenLhs = (String)lhsCursor.value();
    }

    return pattern.matcher(lastSeenLhs).matches();
  }

  @Override
  public TextQueryVariableExpression negate() {
    this.isNegated = !isNegated;
    return this;
  }

  @Override
  public TraversalInfo traversalInfo() {
    return null;
  }

  @Override
  public EvaluationDifficulty evaluationDifficulty() {
    return null;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return null;
  }
}
