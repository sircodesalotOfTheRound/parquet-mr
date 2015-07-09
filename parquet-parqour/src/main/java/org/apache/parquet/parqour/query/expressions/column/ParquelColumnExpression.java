package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelNamedColumnExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelWildcardExpressionBacktrackRule;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
public abstract class ParquelColumnExpression extends ParquelExpression {
  private static final ParquelBacktrackingRuleSet<ParquelColumnExpression> rules = new ParquelBacktrackingRuleSet<ParquelColumnExpression>()
    .add(new ParquelNamedColumnExpressionBacktrackRule())
    .add(new ParquelWildcardExpressionBacktrackRule());

  public ParquelColumnExpression(ParquelExpression parent, ParquelLexer lexer, ParquelExpressionType type) {
    super(parent, lexer, type);
  }

  /*
  @Override
  public ParquelCollection<ParquelExpression> children() {
    return null;
  }*/

  public static ParquelColumnExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return rules.read(parent, lexer);
  }

  public static boolean canParse(ParquelExpression parent, ParquelLexer lexer) {
    return rules.canParse(parent, lexer);
  }

  public boolean isWildcardColumn() {
    return this.is(ParquelExpressionType.WILDCARD);
  }

  public boolean isNamedColumn() {
    return this.is(ParquelExpressionType.NAMED_COLUMN);
  }
}
