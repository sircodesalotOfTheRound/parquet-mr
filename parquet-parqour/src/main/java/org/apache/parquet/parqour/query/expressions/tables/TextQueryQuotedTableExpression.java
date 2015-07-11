package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelPunctuationToken;
import org.apache.parquet.parqour.query.tokens.ParquelToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class TextQueryQuotedTableExpression extends TextQueryTableExpression {
  private final TextQueryAppendableCollection<ParquelToken> expressionTokens;
  private final String expressionAsString;

  public TextQueryQuotedTableExpression(TextQueryExpression parent, ParquelLexer lexer) {
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

  private void validateLexing(TextQueryExpression parent, ParquelLexer lexer) {
    if (!lexer.currentIs(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.SINGLE_QUOTE)) {
      throw new ParquelException("Named table expressions must start with a single quote.");
    }
  }

  private TextQueryAppendableCollection<ParquelToken> readExpression(ParquelLexer lexer) {
    TextQueryAppendableCollection<ParquelToken> tokens = new TextQueryAppendableCollection<ParquelToken>();

    lexer.temporarilyIncludeWhitespaces();
    lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.SINGLE_QUOTE);
    while (!lexer.isEof() && !lexer.currentIs(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.SINGLE_QUOTE)) {
      tokens.add(lexer.readCurrentAndAdvance());
    }

    lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.SINGLE_QUOTE);
    lexer.revertToPreviousWhitespaceInclusionState();
    return tokens;
  }

  private String convertExpressionToString(TextQueryAppendableCollection<ParquelToken> expressionTokens) {
    StringBuilder builder = new StringBuilder();
    for (ParquelToken token : expressionTokens) {
      builder.append(token.toString());
    }

    return builder.toString().trim();
  }

  public static TextQueryQuotedTableExpression read(TextQueryExpression parent, ParquelLexer lexer) {
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
