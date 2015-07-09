package org.apache.parquet.parqour.ingest.plan.predicates.traversal;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;

/**
 * Created by sircodesalot on 6/8/15.
 */
public class TraversalInfo {
  private final ColumnPredicate node;
  private final TraversalPreference traversalPreference;
  private final PredicateEvaluationDifficulty maxEvaluationDifficulty;
  private final int orCount;
  private final int andCount;
  private final int depth;

  // Some paths will take less time than others, so the 'left' path
  // is the direction that will require the least amount of work, while the
  // right path will require the most. We name these paths 'chosenLeft' and 'chosenRight'.
  private final ColumnPredicate chosenLhsNode;
  private final ColumnPredicate chosenRhsNode;

  public TraversalInfo(ColumnPredicate.LeafColumnPredicate node) {
    this.node = node;
    this.traversalPreference = TraversalPreference.THIS_NODE;
    this.maxEvaluationDifficulty = PredicateEvaluationDifficulty.determineFromColumnDescriptor(node);
    this.andCount = 0;
    this.orCount = 0;
    this.depth = 1;

    // Preferred and Non-Preferred are synonymous with 'left' and 'right'.
    // But some paths will take less time than others.
    this.chosenLhsNode = node;
    this.chosenRhsNode = node;
  }

  public TraversalInfo(ColumnPredicate.LogicColumnPredicate node) {
    this.node = node;
    this.depth = computeDepth(node);
    this.maxEvaluationDifficulty = determinePredicateDifficulty(node);
    this.orCount = calculateOrCount(node);
    this.andCount = calculateAndCount(node);
    this.traversalPreference = computeTraversalPreferenceForBinaryLogicNode(node);
    this.chosenLhsNode = determinePrefferedTraversalNode(node);
    this.chosenRhsNode = determineNonPreferredTraversalNode(node);
  }

  private ColumnPredicate determinePrefferedTraversalNode(ColumnPredicate.LogicColumnPredicate node) {
    if (this.traversalPreference == TraversalPreference.THIS_NODE) {
      return node;
    } else if (this.traversalPreference == TraversalPreference.LEFT_NODE) {
      return node.lhs();
    } else {
      return node.rhs();
    }
  }

  private ColumnPredicate determineNonPreferredTraversalNode(ColumnPredicate.LogicColumnPredicate node) {
    if (this.traversalPreference == TraversalPreference.THIS_NODE) {
      return node;
    } else if (this.traversalPreference != TraversalPreference.LEFT_NODE) {
      return node.lhs();
    } else {
      return node.rhs();
    }
  }

  private int calculateAndCount(ColumnPredicate.LogicColumnPredicate node) {
    int lhsAndCount = node.lhs().traversalInfo().andCount();
    int rhsAndCount = node.rhs().traversalInfo().andCount();
    if (node.predicateType() == ColumnPredicateType.AND) {
      return lhsAndCount + rhsAndCount + 1;
    } else {
      return lhsAndCount + rhsAndCount;
    }
  }

  private int calculateOrCount(ColumnPredicate.LogicColumnPredicate node) {
    int lhsOrCount = node.lhs().traversalInfo().orCount();
    int rhsOrCount = node.rhs().traversalInfo().orCount();
    if (node.predicateType() == ColumnPredicateType.OR) {
      return lhsOrCount + rhsOrCount + 1;
    } else {
      return lhsOrCount + rhsOrCount;
    }
  }

  private PredicateEvaluationDifficulty determinePredicateDifficulty(ColumnPredicate.LogicColumnPredicate node) {
    PredicateEvaluationDifficulty lhs = node.lhs().traversalInfo().predicateEvaluationDifficulty();
    PredicateEvaluationDifficulty rhs = node.rhs().traversalInfo().predicateEvaluationDifficulty();

    if (lhs.compareTo(rhs) > 0) {
      return lhs;
    } else {
      return rhs;
    }
  }

  private TraversalPreference computeTraversalPreferenceForBinaryLogicNode(ColumnPredicate.LogicColumnPredicate logicNode) {
    TraversalInfo lhs = logicNode.lhs().traversalInfo();
    TraversalInfo rhs = logicNode.rhs().traversalInfo();

    // Always choose the path of least evaluation difficulty.
    if (!lhs.predicateEvaluationDifficulty().equals(rhs.predicateEvaluationDifficulty())) {
      if (lhs.predicateEvaluationDifficulty().compareTo(rhs.predicateEvaluationDifficulty()) < 0) {
        return TraversalPreference.LEFT_NODE;
      } else {
        return TraversalPreference.RIGHT_NODE;
      }
    }

    // Tiebreaker: Choose the path of least distance:
    if (lhs.depth() != rhs.depth()) {
      if (lhs.depth() < rhs.depth()) {
        return TraversalPreference.LEFT_NODE;
      } else {
        return TraversalPreference.RIGHT_NODE;
      }
    }

    if (lhs.andCount() != rhs.andCount() || lhs.orCount() != lhs.orCount()) {
      return determinePathBasedOnHomogenity(lhs, rhs);
    }

    // Else, default to left node.
    return TraversalPreference.LEFT_NODE;
  }

  // A path that contains the same type of logic nodes (AND, OR)
  // is more likely to succeed all at once (OR), or fail all at once (AND).
  private TraversalPreference determinePathBasedOnHomogenity(TraversalInfo lhs, TraversalInfo rhs) {
    int maxAndCount = Math.max(lhs.andCount(), rhs.andCount());
    int maxOrCount = Math.max(lhs.andCount(), rhs.andCount());

    if (this.node.predicateType() == ColumnPredicateType.AND) {
      maxAndCount += 1;
    } else if (this.node.predicateType() == ColumnPredicateType.OR) {
      maxOrCount += 1;
    }

    int maxCount = Math.max(maxAndCount, maxOrCount);

    if (lhs.andCount() == maxCount || lhs.orCount() == maxCount) {
      return TraversalPreference.LEFT_NODE;
    } else {
      return TraversalPreference.RIGHT_NODE;
    }
  }

  private int computeDepth(ColumnPredicate node) {
    ColumnPredicate.LogicColumnPredicate asLogicPredicate = (ColumnPredicate.LogicColumnPredicate) node;
    int lhsDepth = asLogicPredicate.lhs().traversalInfo().depth();
    int rhsDepth = asLogicPredicate.rhs().traversalInfo().depth();
    return Math.max(lhsDepth, rhsDepth) + 1;
  }

  public int andCount() { return this.andCount; }
  public int orCount() { return this.orCount; }
  public PredicateEvaluationDifficulty predicateEvaluationDifficulty() { return maxEvaluationDifficulty; }
  public TraversalPreference traversalPreference() { return this.traversalPreference; }
  public int depth() { return this.depth; }

  public ColumnPredicate chosenLhsNode() { return this.chosenLhsNode; }
  public ColumnPredicate chosenRhsNode() { return this.chosenRhsNode; }

}
