package org.apache.parquet.parqour.query.visitor;

import org.apache.parquet.parqour.query.expressions.column.TextQueryColumnSetExpression;
import org.apache.parquet.parqour.query.expressions.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.expressions.column.TextQueryWildcardExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQuerySelectStatement;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryWhereExpression;

/**
 * Created by sircodesalot on 6/27/15.
 */
public abstract class TextQueryExpressionVisitor<TReturnType> {
  public TReturnType visit(TextQueryTreeRootExpression textQueryTreeRootExpression) {
    return null;
  }

  public TReturnType visit(TextQuerySelectStatement textQuerySelectStatement) {
    return null;
  }

  public TReturnType visit(TextQueryColumnSetExpression textQueryColumnSetExpression) {
    return null;
  }

  public TReturnType visit(TextQueryNamedColumnExpression textQueryColumnExpression) {
    return null;
  }

  public TReturnType visit(TextQueryWildcardExpression textQueryWildcardExpression) {
    return null;
  }

  public TReturnType visit(TextQueryWhereExpression textQueryWhereExpression) {
    return null;
  }
}
