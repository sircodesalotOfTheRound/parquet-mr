package org.apache.parquet.parqour.query.expressions.column;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelNamedColumnExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelWildcardExpressionBacktrackRule;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/3.
 */
public abstract class TextQueryColumnExpression extends TextQueryVariableExpression {
  private static final ParquelBacktrackingRuleSet<TextQueryColumnExpression> rules = new ParquelBacktrackingRuleSet<TextQueryColumnExpression>()
    .add(new ParquelNamedColumnExpressionBacktrackRule())
    .add(new ParquelWildcardExpressionBacktrackRule());

  public TextQueryColumnExpression(TextQueryExpression parent, ParquelLexer lexer, ParquelExpressionType type) {
    super(parent, lexer, type);
  }

  /*
  @Override
  public ParquelCollection<ParquelExpression> children() {
    return null;
  }*/

  public static TextQueryColumnExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return rules.read(parent, lexer);
  }

  public static boolean canParse(TextQueryExpression parent, ParquelLexer lexer) {
    return rules.canParse(parent, lexer);
  }

  public boolean isWildcardColumn() {
    return this.is(ParquelExpressionType.WILDCARD);
  }

  public boolean isNamedColumn() {
    return this.is(ParquelExpressionType.NAMED_COLUMN);
  }
}
