package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryColumnSetExpression extends TextQueryExpression {
  private final String COLUMNS = "(COLUMNS)";
  private final TextQueryCollection<TextQueryColumnExpression> columns;

  public TextQueryColumnSetExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.COLUMN_SET);
    this.columns = readColumns(lexer);
  }

  private TextQueryCollection<TextQueryColumnExpression> readColumns(TextQueryLexer lexer) {
    lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.SELECT);
    TextQueryAppendableCollection<TextQueryColumnExpression> columns = new TextQueryAppendableCollection<TextQueryColumnExpression>();

    while (TextQueryColumnExpression.canParse(this, lexer)) {
      columns.add(TextQueryColumnExpression.read(this, lexer));

      // If the following isn't a comma, then drop out.
      if (!lexer.isEof() && lexer.currentIs(TextQueryExpressionType.PUNCTUATION, ",")) {
        lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION);
      } else {
        break;
      }
    }

    return columns;
  }

  public boolean containsWildcardColumn() {
    return columns.any(new Predicate<TextQueryColumnExpression>() {
      @Override
      public boolean test(TextQueryColumnExpression column) {
        return column.isWildcardColumn();
      }
    });
  }

  public TextQueryCollection<TextQueryColumnExpression> columns() {
    return this.columns;
  }

  public static TextQueryColumnSetExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryColumnSetExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return COLUMNS;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

}
