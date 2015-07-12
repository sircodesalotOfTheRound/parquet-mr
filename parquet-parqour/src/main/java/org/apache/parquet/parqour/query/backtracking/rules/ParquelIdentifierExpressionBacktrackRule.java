package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryIdentifierExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelIdentifierExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelIdentifierExpressionBacktrackRule() {
    super(TextQueryExpressionType.IDENTIFIER);
  }

  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.IDENTIFIER) && !TextQueryKeywordExpression.isKeyword(lexer);
  }

  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryIdentifierExpression.read(parent, lexer);
  }
}
