package org.apache.parquet.parqour.query.backtracking;


import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQuerySelectStatement;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelSelectStatementBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelSelectStatementBacktrackRule() {
    super(ParquelExpressionType.IDENTIFIER);
  }

  public boolean isMatch(TextQueryExpression parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER, TextQueryKeywordExpression.SELECT);
  }

  public TextQueryExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return TextQuerySelectStatement.read(parent, lexer);
  }
}
