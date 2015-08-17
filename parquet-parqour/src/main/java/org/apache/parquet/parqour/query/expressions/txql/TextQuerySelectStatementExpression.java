package org.apache.parquet.parqour.query.expressions.txql;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryStatementExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryColumnSetExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQuerySelectStatementExpression extends TextQueryKeywordExpression implements TextQueryStatementExpression {
  private final TextQueryColumnSetExpression columns;
  private final TextQueryFromExpression from;
  private final TextQueryWhereExpression where;

  public TextQuerySelectStatementExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.SELECT);

    this.columns = readColumns(lexer);
    this.from = readTables(lexer);
    this.where = readPredicates(lexer);
  }

  private TextQueryColumnSetExpression readColumns(TextQueryLexer lexer) {
    return TextQueryColumnSetExpression.read(this, lexer);
  }

  private TextQueryFromExpression readTables(TextQueryLexer lexer) {
    if (TextQueryFromExpression.canParse(this, lexer)) {
      return TextQueryFromExpression.read(this, lexer);
    } else {
      return null;
    }
  }

  private TextQueryWhereExpression readPredicates(TextQueryLexer lexer) {
    if (TextQueryWhereExpression.canParse(this, lexer)) {
      return TextQueryWhereExpression.read(this, lexer);
    } else {
      return null;
    }
  }

  public TextQueryColumnSetExpression columnSet() {
    return this.columns;
  }

  public TextQueryFromExpression from() {
    return this.from;
  }


  public static TextQuerySelectStatementExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQuerySelectStatementExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return SELECT;
  }

  public TextQueryWhereExpression where() {
    return this.where;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    columns.collectColumnDependencies(collectTo);
    if (from != null) from.collectColumnDependencies(collectTo);
    if (where != null) where.collectColumnDependencies(collectTo);

    return collectTo;
  }

}
