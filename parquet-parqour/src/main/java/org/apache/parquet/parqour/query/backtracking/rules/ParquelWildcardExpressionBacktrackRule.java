package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.column.TextQueryWildcardExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelWildcardExpressionBacktrackRule extends ParquelBacktrackRuleBase {

  public ParquelWildcardExpressionBacktrackRule() {
    super(TextQueryExpressionType.PUNCTUATION);
  }

  @Override
  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.WILDCARD);
  }

  @Override
  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryWildcardExpression.read(parent, lexer);
  }
}
