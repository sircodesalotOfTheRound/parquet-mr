package org.apache.parquet.parqour.query.expressions.variable.constant;

import org.apache.parquet.parqour.cursor.implementations.noniterable.constant.ConstantValueCursor;
import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryNumericToken;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class TextQueryNumericExpression extends TextQueryVariableExpression {
  private TextQueryNumericToken value;

  public TextQueryNumericExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.NUMERIC);

    this.value = readValue(lexer);
  }

  public TextQueryNumericExpression(TextQueryExpression parent, TextQueryNumericToken value) {
    super(parent, TextQueryExpressionType.NUMERIC);

    this.value = value;
  }

  private TextQueryNumericToken readValue(TextQueryLexer lexer) {
    // TODO: Clean this up. Negation should be handled more naturally.
    if (lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.MINUS)) {
      lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.MINUS);
      TextQueryNumericToken numericToken = lexer.readCurrentAndAdvance(TextQueryExpressionType.NUMERIC);

      return new TextQueryNumericToken(numericToken.value().negate());
    } else {
      return lexer.readCurrentAndAdvance(TextQueryExpressionType.NUMERIC);
    }
  }

  public static TextQueryNumericExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryNumericExpression(parent, lexer);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return this;
  }

  @Override
  public TextQueryVariableExpression negate() {
    throw new TextQueryException("Attempted to negate a numeric value");
  }

  @Override
  public String toString() {
    return value.toString();
  }

  public Integer asInteger() {
    return this.value.value().intValue();
  }

  @Override
  public Cursor getCursor() {
    return new ConstantValueCursor(value.toString(), -1, this.value.value().intValue());
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }
}
