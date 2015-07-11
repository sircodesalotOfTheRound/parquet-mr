package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryTableSetExpression extends TextQueryExpression {
  private static final String TABLE_SET = "(TABLES)";
  private final TextQueryCollection<TextQueryTableExpression> tables;

  public TextQueryTableSetExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.TABLE_SET);

    this.tables = readTables(lexer);
  }
/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }
  @Override
  public ParquelCollection<ParquelExpression> children() {
    return null;
    //return tables.castTo(ParquelExpression.class);
  }*/

  private TextQueryCollection<TextQueryTableExpression> readTables(ParquelLexer lexer) {
    TextQueryAppendableCollection<TextQueryTableExpression> tables = new TextQueryAppendableCollection<TextQueryTableExpression>();
    while (!lexer.isEof()) {
      // Read the next entry. Or break on failure.
      if (TextQueryTableExpression.canParse(this, lexer)) {
        tables.add(TextQueryTableExpression.read(this, lexer));
      } else {
        break;
      }

      // If the following isn't a comma, then drop out.
      if (!lexer.isEof() && lexer.currentIs(ParquelExpressionType.PUNCTUATION, ",")) {
        lexer.readCurrentAndAdvance(ParquelExpressionType.PUNCTUATION);
      } else {
        break;
      }
    }

    return tables;
  }

  public TextQueryCollection<TextQueryTableExpression> tables() {
    return this.tables;
  }


  public static boolean canParse(TextQueryExpression parent, ParquelLexer lexer) {
    return parent.is(ParquelExpressionType.FROM);
  }

  public static TextQueryTableSetExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return new TextQueryTableSetExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return TABLE_SET;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
