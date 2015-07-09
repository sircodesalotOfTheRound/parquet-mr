package org.apache.parquet.parqour.query.backtracking.rules;


import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.pql.ParquelKeywordExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.tables.ParquelNamedTableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelNamedTableExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelNamedTableExpressionBacktrackRule() {
    super(ParquelExpressionType.IDENTIFIER);
  }

  @Override
  public boolean isMatch(ParquelExpression parent, ParquelLexer lexer) {
    return (parent != null)
      && parent.is(ParquelExpressionType.TABLE_SET)
      && lexer.currentIs(ParquelExpressionType.IDENTIFIER)
      && !ParquelKeywordExpression.isKeyword(lexer);
  }

  @Override
  public ParquelExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return ParquelNamedTableExpression.read(parent, lexer);
  }
}
