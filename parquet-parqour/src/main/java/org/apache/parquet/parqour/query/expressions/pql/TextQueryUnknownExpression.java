package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryUnknownExpression extends TextQueryExpression {
  private final TextQueryToken token;

  public TextQueryUnknownExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.UNKNOWN);

    this.token = lexer.readCurrentAndAdvance();
  }
/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return null;
  }*/

  public TextQueryToken token() {
    return this.token;
  }

  @Override
  public String toString() {
    return this.token.toString();
  }

  public static TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryUnknownExpression(parent, lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
