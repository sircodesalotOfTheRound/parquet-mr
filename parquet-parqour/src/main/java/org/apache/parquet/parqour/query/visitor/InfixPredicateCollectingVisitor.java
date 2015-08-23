package org.apache.parquet.parqour.query.visitor;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryColumnExpression;
import org.apache.parquet.parqour.query.expressions.variable.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.expressions.variable.constant.TextQueryNumericExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryWhereExpression;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Created by sircodesalot on 7/12/15.
 */
public class InfixPredicateCollectingVisitor extends TextQueryExpressionVisitor<FilterPredicate> {
  private final IngestTree ingestTree;
  private final FilterPredicate predicate;

  public InfixPredicateCollectingVisitor(IngestTree ingestTree, TextQueryWhereExpression whereExpression) {
    this.ingestTree = ingestTree;
    this.predicate = whereExpression.accept(this);
  }

  @Override
  public FilterPredicate visit(TextQueryWhereExpression whereExpression) {
    TextQueryInfixExpression infixExpression = (TextQueryInfixExpression) whereExpression.predicate();

    Operators.Column column = collectColumnFromLhs(infixExpression.lhs());
    Integer comparisonValue = collectValueFromRhs(infixExpression.rhs());

    return generatePredicateFromBinaryExpression(infixExpression.operator(), column, comparisonValue);
  }

  private Operators.Column collectColumnFromLhs(TextQueryExpression lhs) {
    if (lhs.type() != TextQueryExpressionType.NAMED_COLUMN) {
      throw new DataIngestException("Currently WHERE only supports column-operator-constant type expressions");
    }

    TextQueryColumnExpression columnExpression = (TextQueryColumnExpression)lhs;
    TextQueryNamedColumnExpression namedColumnExpression = columnExpression.asNamedColumnExpression();
    String path = namedColumnExpression.path();

    return generateColumnFromPath(path);
  }

  private Operators.Column generateColumnFromPath(String path) {
    IngestNode ingestNode = ingestTree.getIngestNodeByPath(path);
    Type schemaNode = ingestNode.type();

    if (!schemaNode.isPrimitive()) {
      throw new DataIngestException("Currently WHERE only supports predicate on schema leaf-nodes.");
    }

    PrimitiveType schemaNodeAsPrimitiveType = ingestNode.type().asPrimitiveType();
    switch (schemaNodeAsPrimitiveType.getPrimitiveTypeName()) {
      case INT32:
        return FilterApi.intColumn(path);

      default:
        throw new DataIngestException("Currently WHERE only integer columns");
    }
  }

  private Integer collectValueFromRhs(TextQueryExpression rhs) {
    if (rhs.type() != TextQueryExpressionType.NUMERIC) {
      throw new DataIngestException("Currently WHERE only supports constant-integer predicates");
    }

    TextQueryNumericExpression numericExpression = (TextQueryNumericExpression) rhs;
    return numericExpression.asInteger();
  }

  private FilterPredicate generatePredicateFromBinaryExpression(InfixOperator operator, Operators.Column column, Integer comparisonValue) {
    if (operator == InfixOperator.EQUALS) {
      return FilterApi.eq((Operators.IntColumn)column, comparisonValue);
    }

    throw new DataIngestException("Unsupported token type");
  }

  public FilterPredicate predicate() { return this.predicate; }
}
