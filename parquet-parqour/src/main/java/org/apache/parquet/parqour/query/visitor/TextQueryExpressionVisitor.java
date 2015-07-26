package org.apache.parquet.parqour.query.visitor;

import org.apache.parquet.parqour.query.expressions.variable.TextQueryUdfExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryColumnSetExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryWildcardExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQuerySelectStatementExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryWhereExpression;
import org.apache.parquet.parqour.query.expressions.variable.parenthetical.TextQueryParentheticalExpression;

/**
 * Created by sircodesalot on 6/27/15.
 */
public abstract class TextQueryExpressionVisitor<TReturnType> {
  public TReturnType visit(TextQueryTreeRootExpression textQueryTreeRootExpression) {
    return null;
  }

  public TReturnType visit(TextQuerySelectStatementExpression textQuerySelectStatementExpression) {
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

  public TReturnType visit(TextQueryParentheticalExpression textQueryParentheticalExpression) {
    return null;
  }

  public TReturnType visit(TextQueryUdfExpression textQueryUdfExpression) {
    return null;
  }
}
