package org.apache.parquet.parqour.query.backtracking.interfaces;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class TextQueryBacktrackRuleBase implements TextQueryBacktrackRule {
  private final TextQueryExpressionType launchForTokensOfType;

  protected TextQueryBacktrackRuleBase(TextQueryExpressionType launchForTokensOfType) {
    this.launchForTokensOfType = launchForTokensOfType;
  }

  public TextQueryExpressionType launchForTokensOfType() {
    return this.launchForTokensOfType;
  }
}
