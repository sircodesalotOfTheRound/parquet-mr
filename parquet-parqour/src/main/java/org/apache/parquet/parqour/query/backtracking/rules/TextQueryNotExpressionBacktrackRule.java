package org.apache.parquet.parqour.query.backtracking.rules;


import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryNamedTableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.expressions.variable.negation.TextQueryNotExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class TextQueryNotExpressionBacktrackRule extends TextQueryBacktrackRuleBase {
  public TextQueryNotExpressionBacktrackRule() {
    super(TextQueryExpressionType.IDENTIFIER);
  }

  @Override
  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.NOT);
  }

  @Override
  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryNotExpression.read(parent, lexer);
  }
}
