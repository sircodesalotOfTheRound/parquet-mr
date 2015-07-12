package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelMemberExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryWildcardExpression extends TextQueryColumnExpression implements ParquelMemberExpression {
  public TextQueryToken token;

  public TextQueryWildcardExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.WILDCARD);

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

  private TextQueryToken readToken(TextQueryLexer lexer) {
    return lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.WILDCARD);
  }

  public static TextQueryWildcardExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryWildcardExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return TextQueryPunctuationToken.WILDCARD;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

}
