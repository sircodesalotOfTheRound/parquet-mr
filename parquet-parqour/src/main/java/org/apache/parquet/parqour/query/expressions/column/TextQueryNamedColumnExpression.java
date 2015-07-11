package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class TextQueryNamedColumnExpression extends TextQueryColumnExpression {
  private final TextQueryFullyQualifiedNameExpression identifier;

  public TextQueryNamedColumnExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.NAMED_COLUMN);

    this.identifier = readFqn(lexer);
  }

  private TextQueryFullyQualifiedNameExpression readFqn(ParquelLexer lexer) {
    if (!lexer.currentIs(ParquelExpressionType.IDENTIFIER)) {
      throw new ParquelException("Identifier Expressions must be located on identifiers");
    }

    return TextQueryFullyQualifiedNameExpression.read(this, lexer);
  }

  public static TextQueryColumnExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return new TextQueryNamedColumnExpression(parent, lexer);
  }

  public TextQueryFullyQualifiedNameExpression identifier() {
    return identifier;
  }

  @Override
  public String toString() {
    return String.format("%s", this.identifier());
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

}
