package org.apache.parquet.parqour.query.tokenization;

import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelToken;
import org.junit.Test;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TestLexer {
  @Test
  public void testLexer() {
    ParquelLexer lexer = new ParquelLexer("select * from table where x = 10", true);

    assert (testAndAdvance(lexer, ParquelExpressionType.IDENTIFIER, TextQueryKeywordExpression.SELECT));
    assert (testAndAdvance(lexer, ParquelExpressionType.PUNCTUATION));
    assert (testAndAdvance(lexer, ParquelExpressionType.IDENTIFIER, TextQueryKeywordExpression.FROM));
    assert (testAndAdvance(lexer, ParquelExpressionType.IDENTIFIER, "table"));
    assert (testAndAdvance(lexer, ParquelExpressionType.IDENTIFIER, TextQueryKeywordExpression.WHERE));
    assert (testAndAdvance(lexer, ParquelExpressionType.IDENTIFIER, "x"));
    assert (testAndAdvance(lexer, ParquelExpressionType.PUNCTUATION, "="));
    assert (testAndAdvance(lexer, ParquelExpressionType.NUMERIC, "10"));
  }

  private <T extends ParquelToken> boolean testAndAdvance(ParquelLexer lexer, ParquelExpressionType type, String representation) {
    if (lexer.currentIs(type, representation)) {
      lexer.advance();
      return true;
    } else {
      return false;
    }
  }


  private <T extends ParquelToken> boolean testAndAdvance(ParquelLexer lexer, ParquelExpressionType type) {
    if (lexer.currentIs(type)) {
      lexer.advance();
      return true;
    } else {
      return false;
    }
  }
}
