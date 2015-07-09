package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.pql.ParquelFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelNamedTableExpression extends ParquelTableExpression {
  private final ParquelFullyQualifiedNameExpression fqn;

  public ParquelNamedTableExpression(ParquelExpression parent, ParquelLexer lexer) {
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

  private void validateLexing(ParquelExpression parent, ParquelLexer lexer) {
    if (!lexer.currentIs(ParquelExpressionType.IDENTIFIER)) {
      throw new ParquelException("Named table expressions must read ");
    } else if (!parent.is(ParquelExpressionType.TABLE_SET)) {
      throw new ParquelException("Parent of ParquelTableExpression must be ParquelTableSetExpression");
    }
  }

  private ParquelFullyQualifiedNameExpression readFqn(ParquelLexer lexer) {
    return ParquelFullyQualifiedNameExpression.read(this, lexer);
  }

  public static ParquelNamedTableExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelNamedTableExpression(parent, lexer);
  }

  public ParquelFullyQualifiedNameExpression fullyQualifiedName() {
    return this.fqn;
  }

  @Override
  public String toString() {
    return String.format("[%s]", this.fullyQualifiedName());
  }
}
