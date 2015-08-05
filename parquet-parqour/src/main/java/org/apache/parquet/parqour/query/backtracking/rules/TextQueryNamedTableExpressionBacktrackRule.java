package org.apache.parquet.parqour.query.backtracking.rules;


import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryNamedTableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
@Deprecated
public class TextQueryNamedTableExpressionBacktrackRule extends TextQueryBacktrackRuleBase {
  public TextQueryNamedTableExpressionBacktrackRule() {
    super(TextQueryExpressionType.IDENTIFIER);
  }

  @Override
  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return (parent != null)
      && parent.is(TextQueryExpressionType.TABLE_SET)
      && lexer.currentIs(TextQueryExpressionType.IDENTIFIER)
      && !TextQueryKeywordExpression.isKeyword(lexer);
  }

  @Override
  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryNamedTableExpression.read(parent, lexer);
  }
}
