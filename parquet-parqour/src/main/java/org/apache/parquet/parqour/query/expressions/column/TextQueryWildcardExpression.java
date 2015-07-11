package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelMemberExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelPunctuationToken;
import org.apache.parquet.parqour.query.tokens.ParquelToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryWildcardExpression extends TextQueryColumnExpression implements ParquelMemberExpression {
  public ParquelToken token;

  public TextQueryWildcardExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.WILDCARD);

    this.token = readToken(lexer);
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

  private ParquelToken readToken(ParquelLexer lexer) {
    return lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.WILDCARD);
  }

  public static TextQueryWildcardExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return new TextQueryWildcardExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return ParquelPunctuationToken.WILDCARD;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

}
