package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelStatementExpression;
import org.apache.parquet.parqour.query.expressions.column.ParquelColumnSetExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelSelectStatement extends ParquelKeywordExpression implements ParquelStatementExpression {
  private final ParquelColumnSetExpression columns;
  private final ParquelFromExpression from;
  private final ParquelWhereExpression where;

  public ParquelSelectStatement(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.SELECT);

    this.columns = readColumns(lexer);
    this.from = readTables(lexer);
    this.where = readPredicates(lexer);
  }
/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return new ParquelAppendableCollection<ParquelExpression>(columns, from);
  }*/

  private ParquelColumnSetExpression readColumns(ParquelLexer lexer) {
    return ParquelColumnSetExpression.read(this, lexer);
  }

  private ParquelFromExpression readTables(ParquelLexer lexer) {
    if (ParquelFromExpression.canParse(this, lexer)) {
      return ParquelFromExpression.read(this, lexer);
    } else {
      return null;
    }
  }

  private ParquelWhereExpression readPredicates(ParquelLexer lexer) {
    if (ParquelWhereExpression.canParse(this, lexer)) {
      return ParquelWhereExpression.read(this, lexer);
    } else {
      return null;
    }
  }

  public ParquelColumnSetExpression columnSet() {
    return this.columns;
  }

  public ParquelFromExpression from() {
    return this.from;
  }


  public static ParquelSelectStatement read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelSelectStatement(parent, lexer);
  }

  @Override
  public String toString() {
    return SELECT;
  }

  public ParquelWhereExpression where() {
    return this.where;
  }
}
