package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelStatementExpression;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryTableSetExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryFromExpression extends TextQueryKeywordExpression implements ParquelStatementExpression {
  private final TextQueryTableSetExpression tableSet;

  public TextQueryFromExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.FROM);

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

  private TextQueryTableSetExpression readTables(ParquelLexer lexer) {
    lexer.readCurrentAndAdvance(ParquelExpressionType.IDENTIFIER, FROM);
    return TextQueryTableSetExpression.read(this, lexer);
  }

  public static boolean canParse(TextQueryExpression parent,  ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER, FROM);
  }

  public static TextQueryFromExpression read(TextQueryExpression parent, ParquelLexer lexer) {
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
