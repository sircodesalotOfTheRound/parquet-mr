package org.apache.parquet.parqour.query.expressions.txql;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryStatementExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryTableSetExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryFromExpression extends TextQueryKeywordExpression implements TextQueryStatementExpression {
  private final TextQueryTableSetExpression tableSet;

  public TextQueryFromExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.FROM);

    this.tableSet = readTables(lexer);
  }

  private TextQueryTableSetExpression readTables(TextQueryLexer lexer) {
    lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER, FROM);
    return TextQueryTableSetExpression.read(this, lexer);
  }

  public static boolean canParse(TextQueryExpression parent,  TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.IDENTIFIER, FROM);
  }

  public static TextQueryFromExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryFromExpression(parent, lexer);
  }

  public TextQueryTableSetExpression tableSet() {
    return this.tableSet;
  }

  @Override
  public String toString() {
    return this.tableSet.toString();
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return null;
  }

}
