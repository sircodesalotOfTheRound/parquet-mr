package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelNamedTableExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelQuotedTableExpressionBacktrackRule;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/3.
 */
public abstract class TextQueryTableExpression extends TextQueryExpression {
  private static final ParquelBacktrackingRuleSet<TextQueryTableExpression> rules = new ParquelBacktrackingRuleSet<TextQueryTableExpression>()
    .add(new ParquelQuotedTableExpressionBacktrackRule())
    .add(new ParquelNamedTableExpressionBacktrackRule());

  private final ParquelTableExpressionType tableExpressionType;

  public TextQueryTableExpression(TextQueryExpression parent, TextQueryLexer lexer, ParquelTableExpressionType tableExpressionType) {
    super(parent, lexer, TextQueryExpressionType.TABLE);

    this.tableExpressionType = tableExpressionType;
  }

  /*
  @Override
  public ParquelCollection<ParquelExpression> children() {
    return null;
  }
*/
  public static boolean canParse(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.canParse(parent, lexer);
  }

  public TextQueryQuotedTableExpression asQuotedTableExpression() {
    return (TextQueryQuotedTableExpression)this;
  }

  public TextQueryNamedTableExpression asNamedTableExpression() {
    return (TextQueryNamedTableExpression)this;
  }

  public static TextQueryTableExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.read(parent, lexer);
  }

  public ParquelTableExpressionType tableExpressionType() {
    return this.tableExpressionType;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
