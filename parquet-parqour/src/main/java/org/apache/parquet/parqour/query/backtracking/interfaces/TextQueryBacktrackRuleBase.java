package org.apache.parquet.parqour.query.backtracking.interfaces;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class TextQueryBacktrackRuleBase implements TextQueryBacktrackRule {
  private final TextQueryExpressionType launchForTokensOfType;

  protected TextQueryBacktrackRuleBase(TextQueryExpressionType launchForTokensOfType) {
    this.launchForTokensOfType = launchForTokensOfType;
  }

  protected boolean withRollback(TextQueryLexer lexer, TextQueryBacktrackingCallback callback) {
    lexer.setUndoPoint();
    boolean result = callback.apply(lexer);
    lexer.rollbackToUndoPoint();

    return result;
  }

  public TextQueryExpressionType launchForTokensOfType() {
    return this.launchForTokensOfType;
  }
}
