package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class TextQueryExpression extends TextQueryToken {

  private final TextQueryLexer lexer;
  private final TextQueryExpressionType type;
  private TextQueryExpression parent;


  public TextQueryExpression(TextQueryExpression parent, TextQueryLexer lexer, TextQueryExpressionType type) {
    super(lexer.position(), type);
    this.parent = parent;
    this.lexer = lexer;
    this.type = type;
  }

  public TextQueryExpression parent() {
    return this.parent;
  }

  public abstract <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor);
}
