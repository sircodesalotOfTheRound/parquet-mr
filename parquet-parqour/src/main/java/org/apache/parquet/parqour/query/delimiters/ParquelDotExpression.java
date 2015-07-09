package org.apache.parquet.parqour.query.delimiters;

import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelDelimiterExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelPunctuationToken;

/**
 * Created by sircodesalot on 15/4/9.
 */
public class ParquelDotExpression extends ParquelExpression implements ParquelDelimiterExpression {
  private final ParquelPunctuationToken dot;

  public ParquelDotExpression(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.DOT);

    this.dot = readComma(lexer);
  }

  private ParquelPunctuationToken readComma(ParquelLexer lexer) {
    return lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.DOT);
  }


  /*@Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return ParquelCollection.EMPTY;
  }*/

  public static boolean canRead(ParquelExpression parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.DOT);
  }

  public static ParquelDotExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelDotExpression(parent, lexer);
  }
}
