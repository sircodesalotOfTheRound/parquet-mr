package org.apache.parquet.parqour.query.delimiters;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryDelimiterExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/9.
 */
public class TextQueryDotExpression extends TextQueryExpression implements TextQueryDelimiterExpression {
  private final TextQueryPunctuationToken dot;

  public TextQueryDotExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.DOT);

    this.dot = readComma(lexer);
  }

  private TextQueryPunctuationToken readComma(TextQueryLexer lexer) {
    return lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.DOT);
  }


  /*@Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return ParquelCollection.EMPTY;
  }*/

  public static boolean canRead(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.DOT);
  }

  public static TextQueryDotExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryDotExpression(parent, lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
