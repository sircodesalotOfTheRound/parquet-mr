package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class TextQueryNamedTableExpression extends TextQueryTableExpression {
  private final TextQueryFullyQualifiedNameExpression fqn;

  public TextQueryNamedTableExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super (parent, lexer, ParquelTableExpressionType.NAMED);

    this.validateLexing(parent, lexer);
    this.fqn = readFqn(lexer);
  }

  /*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return new ParquelAppendableCollection<ParquelExpression>(fqn);
  }*/

  private void validateLexing(TextQueryExpression parent, TextQueryLexer lexer) {
    if (!lexer.currentIs(TextQueryExpressionType.IDENTIFIER)) {
      throw new TextQueryException("Named table expressions must read ");
    } else if (!parent.is(TextQueryExpressionType.TABLE_SET)) {
      throw new TextQueryException("Parent of ParquelTableExpression must be ParquelTableSetExpression");
    }
  }

  private TextQueryFullyQualifiedNameExpression readFqn(TextQueryLexer lexer) {
    return TextQueryFullyQualifiedNameExpression.read(this, lexer);
  }

  public static TextQueryNamedTableExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryNamedTableExpression(parent, lexer);
  }

  public TextQueryFullyQualifiedNameExpression fullyQualifiedName() {
    return this.fqn;
  }

  @Override
  public String toString() {
    return String.format("[%s]", this.fullyQualifiedName());
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
