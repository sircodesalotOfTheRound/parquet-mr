package org.apache.parquet.parqour.query.backtracking;


import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQuerySelectStatement;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelSelectStatementBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelSelectStatementBacktrackRule() {
    super(TextQueryExpressionType.IDENTIFIER);
  }

  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.SELECT);
  }

  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQuerySelectStatement.read(parent, lexer);
  }
}
