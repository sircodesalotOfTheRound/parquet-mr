package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class TextQueryNamedTableExpression extends TextQueryTableExpression {
  private final TextQueryFullyQualifiedNameExpression fqn;

  public TextQueryNamedTableExpression(TextQueryExpression parent, ParquelLexer lexer) {
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

  private void validateLexing(TextQueryExpression parent, ParquelLexer lexer) {
    if (!lexer.currentIs(ParquelExpressionType.IDENTIFIER)) {
      throw new ParquelException("Named table expressions must read ");
    } else if (!parent.is(ParquelExpressionType.TABLE_SET)) {
      throw new ParquelException("Parent of ParquelTableExpression must be ParquelTableSetExpression");
    }
  }

  private TextQueryFullyQualifiedNameExpression readFqn(ParquelLexer lexer) {
    return TextQueryFullyQualifiedNameExpression.read(this, lexer);
  }

  public static TextQueryNamedTableExpression read(TextQueryExpression parent, ParquelLexer lexer) {
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
