package org.apache.parquet.parqour.query.expressions.variable.constant;

import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.cursor.implementations.noniterable.resolved.ConstantValueCursor;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryIdentifierToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 8/5/15.
 */
public class TextQueryNullExpression extends TextQueryVariableExpression {
  public boolean isNegated = false;
  private final TextQueryIdentifierToken nullExpression;


  public TextQueryNullExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, TextQueryExpressionType.NULL);

    this.nullExpression = readNull(lexer);
  }

  private TextQueryIdentifierToken readNull(TextQueryLexer lexer) {
    return lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.NULL);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return this;
  }

  @Override
  public TextQueryVariableExpression negate() {
    this.isNegated = !isNegated;
    return this;
  }

  @Override
  public void bindToTree(IngestTree tree) {

  }

  @Override
  public EvaluationDifficulty evaluationDifficulty() {
    return null;
  }

  @Override
  public Cursor getCursor() {
    if (!isNegated) {
      return new ConstantValueCursor("null", -1, null);
    } else {
      return new ConstantValueCursor("not-null", -1, new Object());
    }
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return null;
  }

  public static TextQueryNullExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryNullExpression(parent, lexer);
  }
}
