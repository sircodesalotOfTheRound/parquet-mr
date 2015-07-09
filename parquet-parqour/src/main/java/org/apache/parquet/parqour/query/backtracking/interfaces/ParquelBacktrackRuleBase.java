package org.apache.parquet.parqour.query.backtracking.interfaces;

import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class ParquelBacktrackRuleBase implements ParquelBacktrackRule {
  private final ParquelExpressionType launchForTokensOfType;

  protected ParquelBacktrackRuleBase(ParquelExpressionType launchForTokensOfType) {
    this.launchForTokensOfType = launchForTokensOfType;
  }

  public ParquelExpressionType launchForTokensOfType() {
    return this.launchForTokensOfType;
  }
}
