package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelMemberExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelPunctuationToken;
import org.apache.parquet.parqour.query.tokens.ParquelToken;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelWildcardExpression extends ParquelColumnExpression implements ParquelMemberExpression {
  public ParquelToken token;

  public ParquelWildcardExpression(ParquelExpression parent, ParquelLexer lexer) {
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

  public static ParquelWildcardExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelWildcardExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return ParquelPunctuationToken.WILDCARD;
  }
}
