package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelNamedColumnExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelNamedColumnExpressionBacktrackRule() {
    super(ParquelExpressionType.IDENTIFIER);
  }

  @Override
  public boolean isMatch(TextQueryExpression parent, ParquelLexer lexer) {
    // Return true if we're sitting on a name, but that name isn't a keyword.
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER) && !TextQueryKeywordExpression.isKeyword(lexer);
  }

  @Override
  public TextQueryExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return TextQueryNamedColumnExpression.read(parent, lexer);
  }
}
