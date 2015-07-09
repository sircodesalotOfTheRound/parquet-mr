package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.query.collections.ParquelAppendableCollection;
import org.apache.parquet.parqour.query.collections.ParquelCollection;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.pql.ParquelKeywordExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelColumnSetExpression extends ParquelExpression {
  private final String COLUMNS = "(COLUMNS)";
  private final ParquelCollection<ParquelColumnExpression> columns;

  public ParquelColumnSetExpression(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.COLUMN_SET);
    this.columns = readColumns(lexer);
  }
/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return this.columns.castTo(ParquelExpression.class);
  }*/

  private ParquelCollection<ParquelColumnExpression> readColumns(ParquelLexer lexer) {
    lexer.readCurrentAndAdvance(ParquelExpressionType.IDENTIFIER, ParquelKeywordExpression.SELECT);
    ParquelAppendableCollection<ParquelColumnExpression> columns = new ParquelAppendableCollection<ParquelColumnExpression>();

    while (ParquelColumnExpression.canParse(this, lexer)) {
      columns.add(ParquelColumnExpression.read(this, lexer));

      // If the following isn't a comma, then drop out.
      if (!lexer.isEof() && lexer.currentIs(ParquelExpressionType.PUNCTUATION, ",")) {
        lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION);
      } else {
        break;
      }
    }

    return columns;
  }

  public boolean containsWildcardColumn() {
    return columns.any(new Predicate<ParquelColumnExpression>() {
      @Override
      public boolean test(ParquelColumnExpression column) {
        return column.isWildcardColumn();
      }
    });
  }

  public ParquelCollection<ParquelColumnExpression> columns() {
    return this.columns;
  }

  public static ParquelColumnSetExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelColumnSetExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return COLUMNS;
  }
}
