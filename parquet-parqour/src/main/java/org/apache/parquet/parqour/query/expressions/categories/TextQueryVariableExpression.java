package org.apache.parquet.parqour.query.expressions.categories;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelNamedColumnExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelNumericExpressionBacktrackRule;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 6/30/15.
 */
public abstract class TextQueryVariableExpression extends TextQueryExpression {
  private static final ParquelBacktrackingRuleSet<TextQueryVariableExpression> rules = new ParquelBacktrackingRuleSet<TextQueryVariableExpression>()
    .add(new ParquelNumericExpressionBacktrackRule())
    .add(new ParquelNamedColumnExpressionBacktrackRule());

  public TextQueryVariableExpression(TextQueryExpression parent, TextQueryLexer lexer, TextQueryExpressionType type) {
    super(parent, lexer, type);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  public static TextQueryVariableExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.read(parent, lexer);
  }
}
