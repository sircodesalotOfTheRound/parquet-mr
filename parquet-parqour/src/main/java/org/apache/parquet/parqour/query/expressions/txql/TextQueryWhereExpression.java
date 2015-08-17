package org.apache.parquet.parqour.query.expressions.txql;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class TextQueryWhereExpression extends TextQueryExpression {
  private final TextQueryVariableExpression expression;

  private TextQueryWhereExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.WHERE);

    lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.WHERE);
    this.expression = TextQueryVariableExpression.read(this, lexer);
  }

  public static boolean canParse(TextQuerySelectStatementExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.WHERE);
  }

  public static TextQueryWhereExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryWhereExpression(parent, lexer);
  }

  public TextQueryVariableExpression predicate() {
    return expression;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return expression.collectColumnDependencies(collectTo);
  }

}
