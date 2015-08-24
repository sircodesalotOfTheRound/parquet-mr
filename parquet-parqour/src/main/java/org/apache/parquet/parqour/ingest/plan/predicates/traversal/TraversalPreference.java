package org.apache.parquet.parqour.ingest.plan.predicates.traversal;

/**
 * Created by sircodesalot on 6/8/15.
 */
public enum TraversalPreference {
  THIS_NODE,
  LEFT_NODE,
  RIGHT_NODE;

  public static TraversalPreference computeTraversalPreference(EvaluationDifficulty lhs, EvaluationDifficulty rhs) {
    if (lhs.compareTo(rhs) <= 0) {
      return LEFT_NODE;
    } else {
      return RIGHT_NODE;
    }
  }
}
