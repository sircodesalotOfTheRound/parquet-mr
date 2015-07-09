package org.apache.parquet.parqour.query.iface;

import org.apache.parquet.parqour.query.expressions.pql.ParquelTreeRootExpression;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 6/15/15.
 */
public class ParqourSource {
  private final String path;

  public ParqourSource(ParquelTreeRootExpression expression) {
    this.path = determinePathFromExpression(expression);
  }

  private String determinePathFromExpression(ParquelTreeRootExpression expression) {
    if (expression.isFqnExpression()) {
      return expression.asFqnExpression().toString();
    }

    throw new NotImplementedException();
  }

  public String path() { return this.path; }
}
