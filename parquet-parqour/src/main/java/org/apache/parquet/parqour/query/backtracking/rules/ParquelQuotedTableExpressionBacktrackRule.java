package org.apache.parquet.parqour.query.backtracking.rules;


import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackRuleBase;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.tables.TextQueryQuotedTableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class ParquelQuotedTableExpressionBacktrackRule extends ParquelBacktrackRuleBase {
  public ParquelQuotedTableExpressionBacktrackRule() {
    super(TextQueryExpressionType.PUNCTUATION);
  }

  @Override
  public boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer) {
    return (parent != null)
      && lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.SINGLE_QUOTE);
  }

  @Override
  public TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return TextQueryQuotedTableExpression.read(parent, lexer);
  }
}
