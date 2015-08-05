package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackRuleBase;
import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackingCallback;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.variable.constant.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryNumericExpressionBacktrackRule extends TextQueryBacktrackRuleBase {
  public TextQueryNumericExpressionBacktrackRule() {
    super(TextQueryExpressionType.PUNCTUATION, TextQueryExpressionType.NUMERIC);
  }

  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return super.withRollback(lexer, new TextQueryBacktrackingCallback() {
      @Override
      public boolean apply(TextQueryLexer lexer) {
        if (lexer.currentIs(TextQueryExpressionType.NUMERIC)) {
          return true;
        } else {
          if (lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.MINUS)) {
            lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.MINUS);
            return lexer.currentIs(TextQueryExpressionType.NUMERIC);
          }
        }

        return false;
      }
    });
  }

  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryNumericExpression.read(parent, lexer);
  }
}
