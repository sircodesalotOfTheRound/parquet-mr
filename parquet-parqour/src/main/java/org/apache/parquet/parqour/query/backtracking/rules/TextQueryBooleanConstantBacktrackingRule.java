package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.variable.constant.TextQueryBooleanConstantExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 8/5/15.
 */
public class TextQueryBooleanConstantBacktrackingRule extends TextQueryBacktrackRuleBase {
  public TextQueryBooleanConstantBacktrackingRule() {
    super(TextQueryExpressionType.IDENTIFIER);
  }

  @Override
  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return (lexer.currentIs(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.TRUE))
      || (lexer.currentIs(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.FALSE));
  }

  @Override
  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryBooleanConstantExpression.read(parent, lexer);
  }
}
