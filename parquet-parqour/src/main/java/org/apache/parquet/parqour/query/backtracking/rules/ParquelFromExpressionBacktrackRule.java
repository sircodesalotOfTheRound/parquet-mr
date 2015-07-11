package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryFromExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelFromExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelFromExpressionBacktrackRule() {
    super(ParquelExpressionType.IDENTIFIER);
  }

  public boolean isMatch(TextQueryExpression parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER, TextQueryKeywordExpression.FROM);
  }

  public TextQueryExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return TextQueryFromExpression.read(parent, lexer);
  }
}
