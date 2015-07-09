package org.apache.parquet.parqour.query.visitor;

import org.apache.parquet.parqour.query.expressions.ParquelExpression;

/**
 * Created by sircodesalot on 6/27/15.
 */
public interface ParquelNoReturnVisitor {
  void visit(ParquelExpression expression);
}
