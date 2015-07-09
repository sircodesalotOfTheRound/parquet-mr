package org.apache.parquet.parqour.query.backtracking.interfaces;

import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.collections.OneToManyMap;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelBacktrackingRuleSet<TExpressionType> {
  private final OneToManyMap<ParquelExpressionType, ParquelBacktrackRule> rules;

  public ParquelBacktrackingRuleSet() {
    this.rules = new OneToManyMap<ParquelExpressionType, ParquelBacktrackRule>();
  }

  public ParquelBacktrackingRuleSet<TExpressionType> add(ParquelBacktrackRule rule) {
    rules.add(rule.launchForTokensOfType(), rule);
    return this;
  }

  public TExpressionType read(ParquelExpression parent, ParquelLexer lexer) {
    if (lexer.isEof()) {
      throw new ParquelException("Attempt to read past end of stream");
    }

    // Try to find a backtrack rule that matches the current token.
    // If that isn't found, launch all of the backtrack rules (all rules
    // listening to Object.class) -- which is all rules.
    ParquelExpressionType typeOfCurrentToken = lexer.current().type();
    return this.findMatch(typeOfCurrentToken, parent, lexer);
  }

  public boolean canParse(ParquelExpression parent, ParquelLexer lexer) {
    if (lexer.isEof()) {
      return false;
    }

    ParquelExpressionType typeOfCurrentToken = lexer.current().type();
    if (!rules.containsKey(typeOfCurrentToken)) {
      return false;
    } else {
      for (ParquelBacktrackRule rule : this.rules.get(typeOfCurrentToken)) {
        if (rule.isMatch(parent, lexer)) {
          return true;
        }
      }
    }

    // All else fails, return false.
    return false;
  }

  private TExpressionType findMatch (ParquelExpressionType type, ParquelExpression parent, ParquelLexer lexer) {
    if (!canParse(parent, lexer)) {
      return null;
    }
    for (ParquelBacktrackRule rule : this.rules.get(type)) {
      if (rule.isMatch(parent, lexer)) {
        return (TExpressionType)rule.read(parent, lexer);
      }
    }

    return null;
  }
}
