package org.apache.parquet.parqour.ingest.plan.predicates.traversal;

import org.apache.parquet.parqour.exceptions.ColumnPredicateBuilderException;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;

/**
 * Created by sircodesalot on 6/8/15.
 */
public enum PredicateEvaluationDifficulty implements Comparable<PredicateEvaluationDifficulty> {
  EASY(0),
  MEDIUM(1),
  DIFFICULT(2);

  private final int difficultyLevel;

  private PredicateEvaluationDifficulty(int difficultyLevel) {
    this.difficultyLevel = difficultyLevel;
  }

  public int difficultyLevel() {
    return this.difficultyLevel;
  }

  public static PredicateEvaluationDifficulty determineFromColumnDescriptor(ColumnPredicate.LeafColumnPredicate node) {
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
}
