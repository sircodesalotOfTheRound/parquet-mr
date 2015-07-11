package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.column.TextQueryWildcardExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelPunctuationToken;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelWildcardExpressionBacktrackRule extends ParquelBacktrackRuleBase {

  public ParquelWildcardExpressionBacktrackRule() {
    super(ParquelExpressionType.PUNCTUATION);
  }

  @Override
  public boolean isMatch(TextQueryExpression parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.PUNCTUATION, ParquelPunctuationToken.WILDCARD);
  }

  @Override
  public TextQueryExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return TextQueryWildcardExpression.read(parent, lexer);
  }
}
