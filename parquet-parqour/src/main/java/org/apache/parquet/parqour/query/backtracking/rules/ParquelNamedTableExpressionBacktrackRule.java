package org.apache.parquet.parqour.query.backtracking.rules;


import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryNamedTableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelNamedTableExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelNamedTableExpressionBacktrackRule() {
    super(ParquelExpressionType.IDENTIFIER);
  }

  @Override
  public boolean isMatch(TextQueryExpression parent, ParquelLexer lexer) {
    return (parent != null)
      && parent.is(ParquelExpressionType.TABLE_SET)
      && lexer.currentIs(ParquelExpressionType.IDENTIFIER)
      && !TextQueryKeywordExpression.isKeyword(lexer);
  }

  @Override
  public TextQueryExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return TextQueryNamedTableExpression.read(parent, lexer);
  }
}
