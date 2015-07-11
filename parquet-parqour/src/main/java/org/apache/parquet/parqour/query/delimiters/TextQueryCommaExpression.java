package org.apache.parquet.parqour.query.delimiters;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelDelimiterExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelPunctuationToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/9.
 */
public class TextQueryCommaExpression extends TextQueryExpression implements ParquelDelimiterExpression {
  private final ParquelPunctuationToken comma;

  public TextQueryCommaExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.COMMA);

    this.comma = readComma(lexer);
  }

  private ParquelPunctuationToken readComma(ParquelLexer lexer) {
    return lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.COMMA);
  }

  /*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return ParquelCollection.EMPTY;
  }
*/
  public static boolean canRead(TextQueryExpression parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.COMMA);
  }

  public static TextQueryCommaExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return new TextQueryCommaExpression(parent, lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
