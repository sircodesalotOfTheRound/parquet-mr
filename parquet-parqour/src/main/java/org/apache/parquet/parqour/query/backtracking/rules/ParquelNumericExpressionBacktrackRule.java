package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelNumericExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelNumericExpressionBacktrackRule() {
    super(TextQueryExpressionType.NUMERIC);
  }

  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.NUMERIC);
  }

  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryNumericExpression.read(parent, lexer);
  }
}
