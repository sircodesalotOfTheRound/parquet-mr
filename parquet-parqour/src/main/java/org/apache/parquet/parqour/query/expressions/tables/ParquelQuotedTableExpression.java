package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.collections.ParquelAppendableCollection;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelPunctuationToken;
import org.apache.parquet.parqour.query.tokens.ParquelToken;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class ParquelQuotedTableExpression extends ParquelTableExpression {
  private final ParquelAppendableCollection<ParquelToken> expressionTokens;
  private final String expressionAsString;

  public ParquelQuotedTableExpression(ParquelExpression parent, ParquelLexer lexer) {
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

  private void validateLexing(ParquelExpression parent, ParquelLexer lexer) {
    if (!lexer.currentIs(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.SINGLE_QUOTE)) {
      throw new ParquelException("Named table expressions must start with a single quote.");
    }
  }

  private ParquelAppendableCollection<ParquelToken> readExpression(ParquelLexer lexer) {
    ParquelAppendableCollection<ParquelToken> tokens = new ParquelAppendableCollection<ParquelToken>();

    lexer.temporarilyIncludeWhitespaces();
    lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.SINGLE_QUOTE);
    while (!lexer.isEof() && !lexer.currentIs(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.SINGLE_QUOTE)) {
      tokens.add(lexer.readCurrentAndAdvance());
    }

    lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.SINGLE_QUOTE);
    lexer.revertToPreviousWhitespaceInclusionState();
    return tokens;
  }

  private String convertExpressionToString(ParquelAppendableCollection<ParquelToken> expressionTokens) {
    StringBuilder builder = new StringBuilder();
    for (ParquelToken token : expressionTokens) {
      builder.append(token.toString());
    }

    return builder.toString().trim();
  }

  public static ParquelQuotedTableExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelQuotedTableExpression(parent, lexer);
  }

  public String asString() {
    return this.expressionAsString;
  }

  @Override
  public String toString() {
    return this.expressionAsString;
  }
}
