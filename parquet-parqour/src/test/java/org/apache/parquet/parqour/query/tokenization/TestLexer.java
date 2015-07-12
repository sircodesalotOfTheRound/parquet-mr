package org.apache.parquet.parqour.query.tokenization;

import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.junit.Test;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TestLexer {
  @Test
  public void testLexer() {
    TextQueryLexer lexer = new TextQueryLexer("select * from table where x = 10", true);

    assert (testAndAdvance(lexer, TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.SELECT));
    assert (testAndAdvance(lexer, TextQueryExpressionType.PUNCTUATION));
    assert (testAndAdvance(lexer, TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.FROM));
    assert (testAndAdvance(lexer, TextQueryExpressionType.IDENTIFIER, "table"));
    assert (testAndAdvance(lexer, TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.WHERE));
    assert (testAndAdvance(lexer, TextQueryExpressionType.IDENTIFIER, "x"));
    assert (testAndAdvance(lexer, TextQueryExpressionType.PUNCTUATION, "="));
    assert (testAndAdvance(lexer, TextQueryExpressionType.NUMERIC, "10"));
  }

  private <T extends TextQueryToken> boolean testAndAdvance(TextQueryLexer lexer, TextQueryExpressionType type, String representation) {
    if (lexer.currentIs(type, representation)) {
      lexer.advance();
      return true;
    } else {
      return false;
    }
  }


  private <T extends TextQueryToken> boolean testAndAdvance(TextQueryLexer lexer, TextQueryExpressionType type) {
    if (lexer.currentIs(type)) {
      lexer.advance();
      return true;
    } else {
      return false;
    }
  }
}
