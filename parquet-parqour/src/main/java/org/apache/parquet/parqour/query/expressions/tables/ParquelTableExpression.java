package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelNamedTableExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelQuotedTableExpressionBacktrackRule;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
public abstract class ParquelTableExpression extends ParquelExpression {
  private static final ParquelBacktrackingRuleSet<ParquelTableExpression> rules = new ParquelBacktrackingRuleSet<ParquelTableExpression>()
    .add(new ParquelQuotedTableExpressionBacktrackRule())
    .add(new ParquelNamedTableExpressionBacktrackRule());

  private final ParquelTableExpressionType tableExpressionType;

  public ParquelTableExpression(ParquelExpression parent, ParquelLexer lexer, ParquelTableExpressionType tableExpressionType) {
    super(parent, lexer, ParquelExpressionType.TABLE);

    this.tableExpressionType = tableExpressionType;
  }

  /*
  @Override
  public ParquelCollection<ParquelExpression> children() {
    return null;
  }
*/
  public static boolean canParse(ParquelExpression parent, ParquelLexer lexer) {
    return rules.canParse(parent, lexer);
  }

  public ParquelQuotedTableExpression asQuotedTableExpression() {
    return (ParquelQuotedTableExpression)this;
  }

  public ParquelNamedTableExpression asNamedTableExpression() {
    return (ParquelNamedTableExpression)this;
  }

  public static ParquelTableExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return rules.read(parent, lexer);
  }

  public ParquelTableExpressionType tableExpressionType() {
    return this.tableExpressionType;
  }
}
