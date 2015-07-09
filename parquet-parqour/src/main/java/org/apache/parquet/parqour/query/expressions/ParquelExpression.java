package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelToken;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class ParquelExpression extends ParquelToken {

  private final ParquelLexer lexer;
  private final ParquelExpressionType type;
  private ParquelExpression parent;


  public ParquelExpression(ParquelExpression parent, ParquelLexer lexer, ParquelExpressionType type) {
    super(lexer.position(), type);
    this.parent = parent;
    this.lexer = lexer;
    this.type = type;
  }

  public ParquelExpression parent() {
    return this.parent;
  }

/*
  public abstract void accept(HQLNoReturnVisitor visitor);

  public abstract HQLCollection<HQLExpression> children();
  */
}
