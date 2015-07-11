package org.apache.parquet.parqour.query.visitor;

import org.apache.parquet.parqour.query.expressions.column.TextQueryColumnExpression;
import org.apache.parquet.parqour.query.expressions.column.TextQueryColumnSetExpression;
import org.apache.parquet.parqour.query.expressions.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.expressions.column.TextQueryWildcardExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQuerySelectStatement;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryWhereExpression;

/**
 * Created by sircodesalot on 6/27/15.
 */
public interface TextQueryExpressionVisitor<TReturnType> {
   TReturnType visit(TextQueryTreeRootExpression textQueryTreeRootExpression);
   TReturnType visit(TextQuerySelectStatement textQuerySelectStatement);
   TReturnType visit(TextQueryColumnSetExpression textQueryColumnSetExpression);
   TReturnType visit(TextQueryNamedColumnExpression textQueryColumnExpression);
   TReturnType visit(TextQueryWildcardExpression textQueryWildcardExpression);
   TReturnType visit(TextQueryWhereExpression textQueryWhereExpression);
}
