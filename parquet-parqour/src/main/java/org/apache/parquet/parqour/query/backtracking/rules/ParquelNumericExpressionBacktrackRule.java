package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelNumericExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelNumericExpressionBacktrackRule() {
    super(ParquelExpressionType.NUMERIC);
  }

  public boolean isMatch(TextQueryExpression parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.NUMERIC);
  }

  public TextQueryExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return TextQueryNumericExpression.read(parent, lexer);
  }
}
