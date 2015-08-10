package org.apache.parquet.parqour.query.backtracking.interfaces;

import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.tools.OneToManyMap;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryBacktrackingRuleSet<TExpressionType> {
  private final OneToManyMap<TextQueryExpressionType, TextQueryBacktrackRule> rules;

  public TextQueryBacktrackingRuleSet() {
    this.rules = new OneToManyMap<TextQueryExpressionType, TextQueryBacktrackRule>();
  }

  public TextQueryBacktrackingRuleSet<TExpressionType> add(TextQueryBacktrackRule rule) {
    for (TextQueryExpressionType type : rule.launchForTokensOfType()) {
      rules.add(type, rule);
    }

    return this;
  }

  public TExpressionType read(TextQueryExpression parent, TextQueryLexer lexer) {
    if (lexer.isEof()) {
      throw new TextQueryException("Attempt to read past end of stream");
    }

    // Try to find a backtrack rule that matches the current token.
    // If that isn't found, launch all of the backtrack rules (all rules
    // listening to Object.class) -- which is all rules.
    TextQueryExpressionType typeOfCurrentToken = lexer.current().type();
    return this.findMatch(typeOfCurrentToken, parent, lexer);
  }

  public boolean canParse(TextQueryExpression parent, TextQueryLexer lexer) {
    if (lexer.isEof()) {
      return false;
    }

    TextQueryExpressionType typeOfCurrentToken = lexer.current().type();
    if (!rules.containsKey(typeOfCurrentToken)) {
      return false;
    } else {
      for (TextQueryBacktrackRule rule : this.rules.get(typeOfCurrentToken)) {
        if (rule.isMatch(parent, lexer)) {
          return true;
        }
      }
    }

    // All else fails, return false.
    return false;
  }

  private TExpressionType findMatch (TextQueryExpressionType type, TextQueryExpression parent, TextQueryLexer lexer) {
    if (!canParse(parent, lexer)) {
      return null;
    }
    for (TextQueryBacktrackRule rule : this.rules.get(type)) {
      if (rule.isMatch(parent, lexer)) {
        return (TExpressionType)rule.read(parent, lexer);
      }
    }

    return null;
  }
}
