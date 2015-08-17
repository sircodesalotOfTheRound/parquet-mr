package org.apache.parquet.parqour.query.expressions.variable.column;

import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryColumnSetExpression extends TextQueryExpression {
  private final String COLUMNS = "(COLUMNS)";
  private final TransformCollection<TextQueryVariableExpression> columns;

  public TextQueryColumnSetExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.COLUMN_SET);
    this.columns = readColumns(lexer);
  }

  private TransformCollection<TextQueryVariableExpression> readColumns(TextQueryLexer lexer) {
    lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.SELECT);
    return TextQueryVariableExpression.readParameterList(this, lexer);
  }

  public boolean containsWildcardColumn() {
    return columns
      .ofType(TextQueryWildcardExpression.class)
      .any();
  }

  public TransformCollection<TextQueryVariableExpression> columns() {
    return this.columns;
  }

  public static TextQueryColumnSetExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryColumnSetExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return COLUMNS;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    for (TextQueryExpression column : columns) {
      column.collectColumnDependencies(collectTo);
    }

    return collectTo;
  }

}
