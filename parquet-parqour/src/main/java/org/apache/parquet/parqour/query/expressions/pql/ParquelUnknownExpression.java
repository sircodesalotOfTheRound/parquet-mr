package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelToken;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelUnknownExpression extends ParquelExpression {
  private final ParquelToken token;

  public ParquelUnknownExpression(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.UNKNOWN);

    this.token = lexer.readCurrentAndAdvance();
  }
/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return null;
  }*/

  public ParquelToken token() {
    return this.token;
  }

  @Override
  public String toString() {
    return this.token.toString();
  }

  public static ParquelExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelUnknownExpression(parent, lexer);
  }
}
