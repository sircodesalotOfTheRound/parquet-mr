package org.apache.parquet.parqour.ingest.schema;

import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;

/**
 * Created by sircodesalot on 7/29/15.
 */
public class QueryFilePath {
  private final String path;

  public QueryFilePath(TextQueryTreeRootExpression expression) {
    this.path = deriveFromExpression(expression);
  }

  private String deriveFromExpression(TextQueryTreeRootExpression expression) {
    if (expression.isSelectStatement()) {
      return expression.asSelectStatement()
        .from()
        .tableSet()
        .tables()
        .first()
        .toString();
    } else {
      return expression.text();
    }
  }

  public String path() { return this.path; }
}
