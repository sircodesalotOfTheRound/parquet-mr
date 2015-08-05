package org.apache.parquet.parqour.query.expressions.variable.constant;

import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.cursor.implementations.noniterable.constant.ConstantValueCursor;
import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class TextQueryStringExpression extends TextQueryVariableExpression {
  private final TextQueryAppendableCollection<TextQueryToken> expressionTokens;
  private final String expressionAsString;

  public TextQueryStringExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.STRING);

    this.validateLexing(parent, lexer);
    this.expressionTokens = readExpression(lexer);
    this.expressionAsString = convertExpressionToString(expressionTokens);
  }

  public TextQueryStringExpression(TextQueryExpression parent, String string) {
    super(parent, TextQueryExpressionType.STRING);

    this.expressionTokens = new TextQueryAppendableCollection<TextQueryToken>();
    this.expressionAsString = string;
  }

  private void validateLexing(TextQueryExpression parent, TextQueryLexer lexer) {
    if (!lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.SINGLE_QUOTE)) {
      throw new TextQueryException("Named table expressions must start with a single quote.");
    }
  }

  private TextQueryAppendableCollection<TextQueryToken> readExpression(TextQueryLexer lexer) {
    TextQueryAppendableCollection<TextQueryToken> tokens = new TextQueryAppendableCollection<TextQueryToken>();

    lexer.temporarilyIncludeWhitespaces();
    lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.SINGLE_QUOTE);
    while (!lexer.isEof() && !lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.SINGLE_QUOTE)) {
      tokens.add(lexer.readCurrentAndAdvance());
    }

    lexer.revertToPreviousWhitespaceInclusionState();
    lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.SINGLE_QUOTE);
    return tokens;
  }

  private String convertExpressionToString(TextQueryAppendableCollection<TextQueryToken> expressionTokens) {
    StringBuilder builder = new StringBuilder();
    for (TextQueryToken token : expressionTokens) {
      builder.append(token.toString());
    }

    return builder.toString().trim();
  }

  public static TextQueryStringExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryStringExpression(parent, lexer);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return this;
  }

  @Override
  public TextQueryVariableExpression negate() {
    throw new TextQueryException("Attempted to negate a string value");
  }

  public String asString() {
    return this.expressionAsString;
  }

  @Override
  public String toString() {
    return this.expressionAsString;
  }

  @Override
  public Cursor getCursor() {
    return new ConstantValueCursor(asString(), -1, asString());
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
