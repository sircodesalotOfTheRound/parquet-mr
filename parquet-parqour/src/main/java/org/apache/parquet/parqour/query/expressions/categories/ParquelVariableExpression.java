package org.apache.parquet.parqour.query.expressions.categories;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelFullyQualifiedNameExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelNumericExpressionBacktrackRule;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 6/30/15.
 */
public abstract class ParquelVariableExpression extends ParquelExpression {
  private static final ParquelBacktrackingRuleSet<ParquelVariableExpression> rules = new ParquelBacktrackingRuleSet<ParquelVariableExpression>()
    .add(new ParquelNumericExpressionBacktrackRule())
    .add(new ParquelFullyQualifiedNameExpressionBacktrackRule());

  public ParquelVariableExpression(ParquelExpression parent, ParquelLexer lexer, ParquelExpressionType type) {
    super(parent, lexer, type);
  }

  public static ParquelVariableExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return rules.read(parent, lexer);
  }
}
