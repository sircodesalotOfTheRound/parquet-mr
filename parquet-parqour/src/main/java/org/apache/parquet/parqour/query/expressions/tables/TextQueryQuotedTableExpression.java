package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class TextQueryQuotedTableExpression extends TextQueryTableExpression {
  private final TextQueryAppendableCollection<TextQueryToken> expressionTokens;
  private final String expressionAsString;

  public TextQueryQuotedTableExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, ParquelTableExpressionType.QUOTED);

    this.validateLexing(parent, lexer);
    this.expressionTokens = readExpression(lexer);
    this.expressionAsString = convertExpressionToString(expressionTokens);
  }


  /*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return new ParquelAppendableCollection<ParquelExpression>(fqn);
  }*/

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

    lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.SINGLE_QUOTE);
    lexer.revertToPreviousWhitespaceInclusionState();
    return tokens;
  }

  private String convertExpressionToString(TextQueryAppendableCollection<TextQueryToken> expressionTokens) {
    StringBuilder builder = new StringBuilder();
    for (TextQueryToken token : expressionTokens) {
      builder.append(token.toString());
    }

    return builder.toString().trim();
  }

  public static TextQueryQuotedTableExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryQuotedTableExpression(parent, lexer);
  }

  public String asString() {
    return this.expressionAsString;
  }

  @Override
  public String toString() {
    return this.expressionAsString;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
