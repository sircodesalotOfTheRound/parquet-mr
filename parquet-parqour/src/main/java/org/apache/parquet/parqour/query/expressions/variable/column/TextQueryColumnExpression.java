package org.apache.parquet.parqour.query.expressions.variable.column;

import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.TextQueryNamedColumnExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.TextQueryWildcardExpressionBacktrackRule;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 15/4/3.
 */
public abstract class TextQueryColumnExpression extends TextQueryTestablePredicateExpression {
  private static final TextQueryBacktrackingRuleSet<TextQueryColumnExpression> rules = new TextQueryBacktrackingRuleSet<TextQueryColumnExpression>()
    .add(new TextQueryNamedColumnExpressionBacktrackRule())
    .add(new TextQueryWildcardExpressionBacktrackRule());

  public TextQueryColumnExpression(TextQueryExpression parent, TextQueryLexer lexer, TextQueryExpressionType type) {
    super(parent, type);
  }

  public static TextQueryColumnExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.read(parent, lexer);
  }

  @Override
  public void bindToTree(IngestTree tree) {

  }

  @Override
  public EvaluationDifficulty evaluationDifficulty() {
    return null;
  }

  public static boolean canParse(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.canParse(parent, lexer);
  }

  public TextQueryNamedColumnExpression asNamedColumnExpression() {
    return (TextQueryNamedColumnExpression) this;
  }

  public TextQueryWildcardExpression asWildcardExpression() {
    return (TextQueryWildcardExpression) this;
  }

  public boolean isWildcardColumn() {
    return this.is(TextQueryExpressionType.WILDCARD);
  }

  public boolean isNamedColumn() {
    return this.is(TextQueryExpressionType.NAMED_COLUMN);
  }
}
