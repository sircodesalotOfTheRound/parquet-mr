package org.apache.parquet.parqour.query.expressions.variable.column;

import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.TraversalInfo;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryFullyQualifiedNameExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 15/4/3.
 */
public class TextQueryNamedColumnExpression extends TextQueryColumnExpression {
  private final TextQueryFullyQualifiedNameExpression identifier;
  private boolean isNegated = false;

  public TextQueryNamedColumnExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.NAMED_COLUMN);

    this.identifier = readFqn(lexer);
  }

  private TextQueryFullyQualifiedNameExpression readFqn(TextQueryLexer lexer) {
    if (!lexer.currentIs(TextQueryExpressionType.IDENTIFIER)) {
      throw new TextQueryException("Identifier Expressions must be located on identifiers");
    }

    return TextQueryFullyQualifiedNameExpression.read(this, lexer);
  }

  public static TextQueryColumnExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryNamedColumnExpression(parent, lexer);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return null;
  }

  @Override
  public TextQueryVariableExpression negate() {
    this.isNegated = !isNegated;
    return this;
  }

  @Override
  public TraversalInfo traversalInfo() {
    return null;
  }

  @Override
  public EvaluationDifficulty evaluationDifficulty() {
    return null;
  }

  public boolean isNegated() { return this.isNegated; }

  public TextQueryFullyQualifiedNameExpression identifier() {
    return identifier;
  }

  @Override
  public String toString() {
    return String.format("%s", this.identifier());
  }

  public String path() {
    return this.identifier.toString();
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    collectTo.add(identifier.toString());
    return collectTo;
  }

}
