package org.apache.parquet.parqour.query.expressions.categories;

import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.TraversalInfo;
import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.*;
import org.apache.parquet.parqour.tools.TransformList;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;

/**
 * Created by sircodesalot on 6/30/15.
 */
public abstract class TextQueryVariableExpression extends TextQueryExpression {
  private static final TextQueryBacktrackingRuleSet<TextQueryVariableExpression> rules = new TextQueryBacktrackingRuleSet<TextQueryVariableExpression>()
    .add(new TextQueryParentheticalExpressionBacktrackRule())
    .add(new TextQueryBooleanConstantBacktrackingRule())
    .add(new TextQueryNumericExpressionBacktrackRule())
    .add(new TextQueryNullExpressionBacktrackingRule())
    .add(new TextQueryNotExpressionBacktrackRule())
    .add(new TextQueryUdfExpressionBacktrackRule())
    .add(new TextQueryNamedColumnExpressionBacktrackRule())
    .add(new TextQueryStringExpressionBacktrackRule())
    .add(new TextQueryWildcardExpressionBacktrackRule());

  public TextQueryVariableExpression(TextQueryExpression parent, TextQueryExpressionType type) {
    super(parent, type);
  }

  public TextQueryVariableExpression(TextQueryExpression parent, TextQueryLexer lexer, TextQueryExpressionType type) {
    super(parent, lexer, type);
  }

  public static boolean canParse(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.canParse(parent, lexer);
  }

  public static TextQueryVariableExpression readIgnoringInfixExpressions(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.read(parent, lexer);
  }

  public static TextQueryVariableExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    lexer.setUndoPoint();
    TextQueryVariableExpression variableExpression = rules.read(parent, lexer);

    if (InfixOperator.isInfixToken(lexer)) {
      lexer.rollbackToUndoPoint();
      return TextQueryInfixExpression.read(parent, lexer);
    } else {
      lexer.clearUndoPoint();
      return variableExpression;
    }
  }

  public static TransformCollection<TextQueryVariableExpression> readParameterList(TextQueryExpression parent, TextQueryLexer lexer) {
    TransformList<TextQueryVariableExpression> parameters = new TransformList<TextQueryVariableExpression>();

    while (TextQueryVariableExpression.canParse(parent, lexer)) {
      parameters.add(TextQueryVariableExpression.read(parent, lexer));

      // If the following isn't a comma, then drop out.
      if (!lexer.isEof() && lexer.currentIs(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.COMMA)) {
        lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION);
      } else {
        break;
      }
    }

    return parameters;
  }

  public abstract TextQueryVariableExpression simplify(TextQueryExpression parent);
  public abstract TextQueryVariableExpression negate();

  @Deprecated
  public abstract TraversalInfo traversalInfo();
  public abstract EvaluationDifficulty evaluationDifficulty();

  public Cursor getCursor() { return null; }
}
