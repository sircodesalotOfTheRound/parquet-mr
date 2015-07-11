package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class TextQueryExpression extends ParquelToken {

  private final ParquelLexer lexer;
  private final ParquelExpressionType type;
  private TextQueryExpression parent;


  public TextQueryExpression(TextQueryExpression parent, ParquelLexer lexer, ParquelExpressionType type) {
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
