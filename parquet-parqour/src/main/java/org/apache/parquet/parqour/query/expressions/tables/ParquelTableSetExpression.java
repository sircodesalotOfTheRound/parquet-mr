package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.query.collections.ParquelAppendableCollection;
import org.apache.parquet.parqour.query.collections.ParquelCollection;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelTableSetExpression extends ParquelExpression {
  private static final String TABLE_SET = "(TABLES)";
  private final ParquelCollection<ParquelTableExpression> tables;

  public ParquelTableSetExpression(ParquelExpression parent, ParquelLexer lexer) {
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

  private ParquelCollection<ParquelTableExpression> readTables(ParquelLexer lexer) {
    ParquelAppendableCollection<ParquelTableExpression> tables = new ParquelAppendableCollection<ParquelTableExpression>();
    while (!lexer.isEof()) {
      // Read the next entry. Or break on failure.
      if (ParquelTableExpression.canParse(this, lexer)) {
        tables.add(ParquelTableExpression.read(this, lexer));
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

  public ParquelCollection<ParquelTableExpression> tables() {
    return this.tables;
  }


  public static boolean canParse(ParquelExpression parent, ParquelLexer lexer) {
    return parent.is(ParquelExpressionType.FROM);
  }

  public static ParquelTableSetExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelTableSetExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return TABLE_SET;
  }
}
