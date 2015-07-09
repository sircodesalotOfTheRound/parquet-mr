package org.apache.parquet.parqour.query.backtracking;


import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.pql.ParquelKeywordExpression;
import org.apache.parquet.parqour.query.expressions.pql.ParquelSelectStatement;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelSelectStatementBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelSelectStatementBacktrackRule() {
    super(ParquelExpressionType.IDENTIFIER);
  }

  public boolean isMatch(ParquelExpression parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER, ParquelKeywordExpression.SELECT);
  }

  public ParquelExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return ParquelSelectStatement.read(parent, lexer);
  }
}
