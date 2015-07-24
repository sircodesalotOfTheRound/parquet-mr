package org.apache.parquet.parqour.query.visitor;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryColumnSetExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryWildcardExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQuerySelectStatementExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryWhereExpression;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 7/11/15.
 */
public class TextQueryColumnCollectingVisitor extends TextQueryExpressionVisitor<Iterable<TextQueryNamedColumnExpression>> {
  private boolean containsAWildcardExpression = false;
  private final TextQueryAppendableCollection<TextQueryNamedColumnExpression> columns
    = new TextQueryAppendableCollection<TextQueryNamedColumnExpression>();

  public TextQueryColumnCollectingVisitor(TextQueryExpression expression) {
    this.collectColumns(expression);
  }

  private List<TextQueryNamedColumnExpression> collectColumns(TextQueryExpression expression) {
    List<TextQueryNamedColumnExpression> columnExpressions = new ArrayList<TextQueryNamedColumnExpression>();
    expression.accept(this);

    return columnExpressions;
  }


  @Override
  public Iterable<TextQueryNamedColumnExpression> visit(TextQueryTreeRootExpression rootExpression) {
    if (rootExpression.containsSelectExpression()) {
      rootExpression.asSelectStatement().accept(this);
    }

    return null;
  }

  @Override
  public Iterable<TextQueryNamedColumnExpression> visit(TextQuerySelectStatementExpression selectStatement) {
    if (selectStatement.columnSet() != null) {
      selectStatement.columnSet().accept(this);
    }

    if (selectStatement.where() != null) {
      selectStatement.where().accept(this);
    }

    return null;
  }

  @Override
  public Iterable<TextQueryNamedColumnExpression> visit(TextQueryColumnSetExpression columnSetExpression) {
    for (TextQueryVariableExpression column : columnSetExpression.columns()) {
      column.accept(this);
    }

    return null;
  }

  @Override
  public Iterable<TextQueryNamedColumnExpression> visit(TextQueryNamedColumnExpression namedColumnExpression) {
    columns.add(namedColumnExpression);
    return null;
  }

  @Override
  public Iterable<TextQueryNamedColumnExpression> visit(TextQueryWildcardExpression wildcardExpression) {
    this.containsAWildcardExpression = true;
    return null;
  }

  @Override
  public Iterable<TextQueryNamedColumnExpression> visit(TextQueryWhereExpression whereExpression) {
    whereExpression.infixExpression().accept(this);
    return null;
  }

  public Iterable<String> columns() {
    return columns.map(new Projection<TextQueryNamedColumnExpression, String>() {
      @Override
      public String apply(TextQueryNamedColumnExpression column) {
        return column.identifier().toString();
      }
    });
  }

  public boolean containsAWildcardExpression() {
    return this.containsAWildcardExpression;
  }
}
