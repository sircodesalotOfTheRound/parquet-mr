package org.apache.parquet.parqour.query.delimiters;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelDelimiterExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/9.
 */
public class TextQueryCommaExpression extends TextQueryExpression implements ParquelDelimiterExpression {
  private final TextQueryPunctuationToken comma;

  public TextQueryCommaExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.COMMA);

    this.comma = readComma(lexer);
  }

  private TextQueryPunctuationToken readComma(TextQueryLexer lexer) {
    return lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.COMMA);
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
  public static boolean canRead(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.COMMA);
  }

  public static TextQueryCommaExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryCommaExpression(parent, lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
