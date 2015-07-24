package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackRuleBase;
import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackingCallback;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.variable.TextQueryUdfExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryUdfExpressionBacktrackRule extends TextQueryBacktrackRuleBase {
  public TextQueryUdfExpressionBacktrackRule() {
    super(TextQueryExpressionType.IDENTIFIER);
  }

  public boolean isMatch(final TextQueryExpression parent, final TextQueryLexer lexer) {
    return super.withRollback(lexer, new TextQueryBacktrackingCallback() {
      @Override
      public boolean apply(TextQueryLexer lexer) {
        if (lexer.currentIs(TextQueryExpressionType.IDENTIFIER)) {
          TextQueryFullyQualifiedNameExpression.read(parent, lexer);
          return lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.OPEN_PARENS);
        }

        return false;
      }
    });
  }

  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryUdfExpression.read(parent, lexer);
  }
}
