package org.apache.parquet.parqour.query.backtracking.interfaces;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class ParquelBacktrackRuleBase implements ParquelBacktrackRule {
  private final TextQueryExpressionType launchForTokensOfType;

  protected ParquelBacktrackRuleBase(TextQueryExpressionType launchForTokensOfType) {
    this.launchForTokensOfType = launchForTokensOfType;
  }

  public TextQueryExpressionType launchForTokensOfType() {
    return this.launchForTokensOfType;
  }
}
