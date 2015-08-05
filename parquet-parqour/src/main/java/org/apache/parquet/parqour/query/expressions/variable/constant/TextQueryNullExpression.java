package org.apache.parquet.parqour.query.expressions.variable.constant;

import com.sun.org.apache.xpath.internal.compiler.Keywords;
import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.cursor.implementations.noniterable.resolved.ConstantValueCursor;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryIdentifierExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryIdentifierToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 8/5/15.
 */
public class TextQueryNullExpression extends TextQueryVariableExpression {
  public boolean isNegated = false;
  private final TextQueryIdentifierToken nullExpression;


  public TextQueryNullExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, TextQueryExpressionType.NULL);

    this.nullExpression = readNull(lexer);
  }

  private TextQueryIdentifierToken readNull(TextQueryLexer lexer) {
    return lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.NULL);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return this;
  }

  @Override
  public TextQueryVariableExpression negate() {
    this.isNegated = !isNegated;
    return this;
  }

  @Override
  public Cursor getCursor() {
    if (!isNegated) {
      return new ConstantValueCursor("null", -1, null);
    } else {
      return new ConstantValueCursor("not-null", -1, new Object());
    }
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  public static TextQueryNullExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryNullExpression(parent, lexer);
  }
}
