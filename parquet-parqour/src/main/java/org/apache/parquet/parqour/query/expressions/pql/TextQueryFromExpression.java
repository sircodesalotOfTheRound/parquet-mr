package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelStatementExpression;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryTableSetExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryFromExpression extends TextQueryKeywordExpression implements ParquelStatementExpression {
  private final TextQueryTableSetExpression tableSet;

  public TextQueryFromExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.FROM);

    this.tableSet = readTables(lexer);
  }
/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return new ParquelAppendableCollection<ParquelExpression>(tableSet);
  }*/

  private TextQueryTableSetExpression readTables(TextQueryLexer lexer) {
    lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER, FROM);
    return TextQueryTableSetExpression.read(this, lexer);
  }

  public static boolean canParse(TextQueryExpression parent,  TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.IDENTIFIER, FROM);
  }

  public static TextQueryFromExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryFromExpression(parent, lexer);
  }

  public TextQueryTableSetExpression tableSet() {
    return this.tableSet;
  }

  @Override
  public String toString() {
    return FROM;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
