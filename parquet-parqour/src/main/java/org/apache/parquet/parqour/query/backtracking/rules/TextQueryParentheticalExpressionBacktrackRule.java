package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryFromExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.variable.parenthetical.TextQueryParentheticalExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryParentheticalExpressionBacktrackRule extends TextQueryBacktrackRuleBase {
  public TextQueryParentheticalExpressionBacktrackRule() {
    super(TextQueryExpressionType.PUNCTUATION);
  }

  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.OPEN_PARENS);
  }

  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryParentheticalExpression.read(parent, lexer);
  }
}
