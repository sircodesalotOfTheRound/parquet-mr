package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class TextQueryNamedColumnExpression extends TextQueryColumnExpression {
  private final TextQueryFullyQualifiedNameExpression identifier;

  public TextQueryNamedColumnExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.NAMED_COLUMN);

    this.identifier = readFqn(lexer);
  }

  private TextQueryFullyQualifiedNameExpression readFqn(TextQueryLexer lexer) {
    if (!lexer.currentIs(TextQueryExpressionType.IDENTIFIER)) {
      throw new TextQueryException("Identifier Expressions must be located on identifiers");
    }

    return TextQueryFullyQualifiedNameExpression.read(this, lexer);
  }

  public static TextQueryColumnExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryNamedColumnExpression(parent, lexer);
  }

  public TextQueryFullyQualifiedNameExpression identifier() {
    return identifier;
  }

  @Override
  public String toString() {
    return String.format("%s", this.identifier());
  }

  public String path() {
    return this.identifier.toString();
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

}
