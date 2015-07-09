package org.apache.parquet.parqour.query.backtracking.rules;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.pql.ParquelFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.expressions.pql.ParquelKeywordExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelFullyQualifiedNameExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelFullyQualifiedNameExpressionBacktrackRule() {
    super(ParquelExpressionType.IDENTIFIER);
  }

  public boolean isMatch(ParquelExpression parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER) && !ParquelKeywordExpression.isKeyword(lexer);
  }

  public ParquelExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return ParquelFullyQualifiedNameExpression.read(parent, lexer);
  }
}
