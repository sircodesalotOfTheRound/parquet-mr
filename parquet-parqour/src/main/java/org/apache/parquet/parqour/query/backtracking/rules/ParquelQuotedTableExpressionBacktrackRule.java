package org.apache.parquet.parqour.query.backtracking.rules;


import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.tables.ParquelQuotedTableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelPunctuationToken;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelQuotedTableExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelQuotedTableExpressionBacktrackRule() {
    super(ParquelExpressionType.PUNCTUATION);
  }

  @Override
  public boolean isMatch(ParquelExpression parent, ParquelLexer lexer) {
    return (parent != null)
      && lexer.currentIs(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.SINGLE_QUOTE);
  }

  @Override
  public ParquelExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return ParquelQuotedTableExpression.read(parent, lexer);
  }
}
