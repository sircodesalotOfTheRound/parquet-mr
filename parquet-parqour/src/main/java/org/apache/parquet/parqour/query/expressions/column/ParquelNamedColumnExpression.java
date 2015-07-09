package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.pql.ParquelFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelNamedColumnExpression extends ParquelColumnExpression {
  private final ParquelFullyQualifiedNameExpression identifier;

  public ParquelNamedColumnExpression(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.NAMED_COLUMN);

    this.identifier = readFqn(lexer);
  }
/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return new ParquelAppendableCollection<ParquelExpression>(identifier);
  }*/

  private ParquelFullyQualifiedNameExpression readFqn(ParquelLexer lexer) {
    if (!lexer.currentIs(ParquelExpressionType.IDENTIFIER)) {
      throw new ParquelException("Identifier Expressions must be located on identifiers");
    }

    return ParquelFullyQualifiedNameExpression.read(this, lexer);
  }

  public static ParquelColumnExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelNamedColumnExpression(parent, lexer);
  }

  public ParquelFullyQualifiedNameExpression identifier() {
    return identifier;
  }

  @Override
  public String toString() {
    return String.format("[%s]", this.identifier());
  }
}
