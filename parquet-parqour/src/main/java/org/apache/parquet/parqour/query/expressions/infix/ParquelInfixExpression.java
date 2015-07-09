package org.apache.parquet.parqour.query.expressions.infix;

import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelVariableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelToken;

/**
 * Created by sircodesalot on 6/30/15.
 */
public class ParquelInfixExpression extends ParquelVariableExpression {
  private ParquelExpression lhs;
  private ParquelExpression rhs;
  private final ParquelToken operationToken;

  public ParquelInfixExpression(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.INFIX);

    this.lhs = readLhs(lexer);
    this.operationToken = readOperationToken(lexer);
    this.rhs = readRhs(lexer);
  }

  private ParquelExpression readLhs(ParquelLexer lexer) {
    return ParquelVariableExpression.read(this, lexer);
  }

  private ParquelToken readOperationToken(ParquelLexer lexer) {
    return lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION);
  }

  private ParquelExpression readRhs(ParquelLexer lexer) {
    return ParquelVariableExpression.read(this, lexer);
  }

  public ParquelExpression lhs() { return this.lhs; }
  public ParquelExpression rhs() { return this.rhs; }
  public ParquelToken operator() { return this.operationToken; }

  public static ParquelInfixExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelInfixExpression(parent, lexer);
  }
}
