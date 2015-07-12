package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelFullyQualifiedNameExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelFullyQualifiedNameExpressionBacktrackRule() {
    super(TextQueryExpressionType.IDENTIFIER);
  }

  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.IDENTIFIER) && !TextQueryKeywordExpression.isKeyword(lexer);
  }

  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryFullyQualifiedNameExpression.read(parent, lexer);
  }
}
