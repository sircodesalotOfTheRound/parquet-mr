package org.apache.parquet.parqour.query.expressions.variable.constant;

import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.cursor.implementations.noniterable.resolved.ConstantValueCursor;
import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.TraversalInfo;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 8/5/15.
 */
public class TextQueryBooleanConstantExpression extends TextQueryTestablePredicateExpression {
  private Boolean value;

  public TextQueryBooleanConstantExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, TextQueryExpressionType.BOOLEAN);

    this.value = readValue(lexer);
  }

  public boolean readValue(TextQueryLexer lexer)  {
    TextQueryToken token = lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER);
    if (token.is(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.TRUE)) {
      return true;
    } else if (token.is(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.FALSE)) {
      return false;
    }

    throw new TextQueryException("Invalid boolean value");
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return this;
  }

  @Override
  public TextQueryVariableExpression negate() {
    this.value = !value;
    return this;
  }

  @Override
  public TraversalInfo traversalInfo() {
    return null;
  }

  @Override
  public EvaluationDifficulty evaluationDifficulty() {
    return EvaluationDifficulty.EASY;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return null;
  }

  @Override
  public boolean test() {
    return value;
  }

  @Override
  public Cursor getCursor() {
    return new ConstantValueCursor(this.value.toString(), -1, value);
  }

  @Override
  public String toString() {
    return this.value.toString();
  }

  public static TextQueryBooleanConstantExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryBooleanConstantExpression(parent, lexer);
  }

  public boolean value() { return this.value; }
}
