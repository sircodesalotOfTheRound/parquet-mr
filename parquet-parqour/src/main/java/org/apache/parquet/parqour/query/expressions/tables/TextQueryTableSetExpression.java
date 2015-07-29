package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryTableSetExpression extends TextQueryExpression {
  // Todo: this should just be a single table.
  private final TextQueryCollection<TextQueryVariableExpression> tables;

  public TextQueryTableSetExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.TABLE_SET);

    this.tables = readTables(lexer);
  }

  private TextQueryCollection<TextQueryVariableExpression> readTables(TextQueryLexer lexer) {
    TextQueryAppendableCollection<TextQueryVariableExpression> tables = new TextQueryAppendableCollection<TextQueryVariableExpression>();
    while (!lexer.isEof()) {
      // Read the next entry. Or break on failure.
      if (TextQueryVariableExpression.canParse(this, lexer)) {
        tables.add(TextQueryVariableExpression.read(this, lexer));
      } else {
        break;
      }

      // If the following isn't a comma, then drop out.
      if (!lexer.isEof() && lexer.currentIs(TextQueryExpressionType.PUNCTUATION, ",")) {
        lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION);
      } else {
        break;
      }
    }

    return tables;
  }

  public TextQueryCollection<TextQueryVariableExpression> tables() {
    return this.tables;
  }


  public static boolean canParse(TextQueryExpression parent, TextQueryLexer lexer) {
    return parent.is(TextQueryExpressionType.FROM);
  }

  public static TextQueryTableSetExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryTableSetExpression(parent, lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
