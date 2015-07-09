package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelStatementExpression;
import org.apache.parquet.parqour.query.expressions.tables.ParquelTableSetExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelFromExpression extends ParquelKeywordExpression implements ParquelStatementExpression {
  private final ParquelTableSetExpression tableSet;

  public ParquelFromExpression(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.FROM);

    this.tableSet = readTables(lexer);
  }
/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return new ParquelAppendableCollection<ParquelExpression>(tableSet);
  }*/

  private ParquelTableSetExpression readTables(ParquelLexer lexer) {
    lexer.readCurrentAndAdvance(ParquelExpressionType.IDENTIFIER, FROM);
    return ParquelTableSetExpression.read(this, lexer);
  }

  public static boolean canParse(ParquelExpression parent,  ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER, FROM);
  }

  public static ParquelFromExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelFromExpression(parent, lexer);
  }

  public ParquelTableSetExpression tableSet() {
    return this.tableSet;
  }

  @Override
  public String toString() {
    return FROM;
  }
}
