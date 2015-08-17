package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.tools.TransformList;
import org.apache.parquet.parqour.tools.TransformCollection;
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
  private final TransformCollection<TextQueryVariableExpression> tables;

  public TextQueryTableSetExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.TABLE_SET);

    this.tables = readTables(lexer);
  }

  private TransformCollection<TextQueryVariableExpression> readTables(TextQueryLexer lexer) {
    TransformList<TextQueryVariableExpression> tables = new TransformList<TextQueryVariableExpression>();
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

  public TransformCollection<TextQueryVariableExpression> tables() {
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

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return null;
  }

}
