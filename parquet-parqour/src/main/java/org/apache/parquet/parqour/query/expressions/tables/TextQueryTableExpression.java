package org.apache.parquet.parqour.query.expressions.tables;

import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.TextQueryNamedTableExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.TextQueryStringExpressionBacktrackRule;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/3.
 */

@Deprecated
public abstract class TextQueryTableExpression extends TextQueryExpression {
  private static final TextQueryBacktrackingRuleSet<TextQueryTableExpression> rules = new TextQueryBacktrackingRuleSet<TextQueryTableExpression>()
    .add(new TextQueryStringExpressionBacktrackRule())
    .add(new TextQueryNamedTableExpressionBacktrackRule());

  private final ParquelTableExpressionType tableExpressionType;

  public TextQueryTableExpression(TextQueryExpression parent, TextQueryLexer lexer, ParquelTableExpressionType tableExpressionType) {
    super(parent, lexer, TextQueryExpressionType.TABLE);

    this.tableExpressionType = tableExpressionType;
  }

  public static boolean canParse(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.canParse(parent, lexer);
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
