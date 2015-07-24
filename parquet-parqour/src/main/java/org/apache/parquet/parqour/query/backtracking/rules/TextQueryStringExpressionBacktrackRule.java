package org.apache.parquet.parqour.query.backtracking.rules;


import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryStringExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class TextQueryStringExpressionBacktrackRule extends TextQueryBacktrackRuleBase {
  public TextQueryStringExpressionBacktrackRule() {
    super(TextQueryExpressionType.PUNCTUATION);
  }

  @Override
  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return (parent != null)
      && lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.SINGLE_QUOTE);
  }

  @Override
  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryStringExpression.read(parent, lexer);
  }
}
