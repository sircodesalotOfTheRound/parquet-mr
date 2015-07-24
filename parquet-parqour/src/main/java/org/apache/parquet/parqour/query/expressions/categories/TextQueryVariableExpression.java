package org.apache.parquet.parqour.query.expressions.categories;

import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.*;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 6/30/15.
 */
public abstract class TextQueryVariableExpression extends TextQueryExpression {
  private static final TextQueryBacktrackingRuleSet<TextQueryVariableExpression> rules = new TextQueryBacktrackingRuleSet<TextQueryVariableExpression>()
    .add(new TextQueryUdfExpressionBacktrackRule())
    .add(new TextQueryNumericExpressionBacktrackRule())
    .add(new TextQueryNamedColumnExpressionBacktrackRule())
    .add(new TextQueryStringExpressionBacktrackRule())
    .add(new TextQueryWildcardExpressionBacktrackRule());

  public TextQueryVariableExpression(TextQueryExpression parent, TextQueryLexer lexer, TextQueryExpressionType type) {
    super(parent, lexer, type);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  public static boolean canParse(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.canParse(parent, lexer);
  }

  public static TextQueryVariableExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return rules.read(parent, lexer);
  }

  public static TextQueryCollection<TextQueryVariableExpression> readParameterList(TextQueryExpression parent, TextQueryLexer lexer) {
    TextQueryAppendableCollection<TextQueryVariableExpression> parameters = new TextQueryAppendableCollection<TextQueryVariableExpression>();

    while (TextQueryVariableExpression.canParse(parent, lexer)) {
      parameters.add(TextQueryVariableExpression.read(parent, lexer));

      // If the following isn't a comma, then drop out.
      if (!lexer.isEof() && lexer.currentIs(TextQueryExpressionType.PUNCTUATION, ",")) {
        lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION);
      } else {
        break;
      }
    }

    return parameters;
  }
}
