package org.apache.parquet.parqour.ingest.plan.predicates.traversal;

import org.apache.parquet.parqour.exceptions.ColumnPredicateBuilderException;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Created by sircodesalot on 6/8/15.
 */
public enum EvaluationDifficulty implements Comparable<EvaluationDifficulty> {
  EASY(0),
  MEDIUM(1),
  DIFFICULT(2);

  private final int difficultyLevel;

  EvaluationDifficulty(int difficultyLevel) {
    this.difficultyLevel = difficultyLevel;
  }

  public int difficultyLevel() {
    return this.difficultyLevel;
  }

  public static EvaluationDifficulty determineFromPrimitiveTypeName(PrimitiveType.PrimitiveTypeName type) {
    switch (type) {
      case INT32:
        return EASY;
      case INT64:
        return EASY;
      case INT96:
        return EASY;
      case FLOAT:
        return MEDIUM;
      case DOUBLE:
        return MEDIUM;
      case BINARY:
        return DIFFICULT;
      case FIXED_LEN_BYTE_ARRAY:
        return DIFFICULT;
      default:
        throw new ColumnPredicateBuilderException("Invalid node type '%s'.", type);
    }
  }

  @Deprecated
  public static EvaluationDifficulty determineFromColumnDescriptor(ColumnPredicate.LeafColumnPredicate node) {
    switch (node.column().getType()) {
      case INT32:
        return EASY;
      case INT64:
        return EASY;
      case INT96:
        return EASY;
      case FLOAT:
        return MEDIUM;
      case DOUBLE:
        return MEDIUM;
      case BINARY:
        return DIFFICULT;
      case FIXED_LEN_BYTE_ARRAY:
        return DIFFICULT;
      default:
        throw new ColumnPredicateBuilderException("Invalid node type '%s'.", node.column().getType());
    }
  }

  public static EvaluationDifficulty max(EvaluationDifficulty ... difficulties) {
    EvaluationDifficulty result = null;
    for (EvaluationDifficulty difficulty : difficulties) {
      if (result != null) {
        result = (result.compareTo(difficulty) >= 0) ? result : difficulty;
      }  else {
        result = difficulty;
      }
    }

    return result;
  }
}
