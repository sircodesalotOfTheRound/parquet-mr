package org.apache.parquet.parqour.ingest.plan.evaluation;

import org.apache.parquet.parqour.ingest.plan.analysis.PredicateAnalysis;
import org.apache.parquet.parqour.ingest.plan.evaluation.skipchain.SkipChain;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.PredicateTestWayPoint;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.TraversalPreference;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNodeCategory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sircodesalot on 6/8/15.
 */
public class EvaluationPathAnalysis {
  private final List<ColumnPredicate.LeafColumnPredicate> predicateLeaves;
  private final Map<ColumnPredicate.LeafColumnPredicate, Integer> indexesForLeaves;
  private final PredicateTestWayPoint path;
  private final IngestTree tree;
  private final IngestNodeSet nodesAssociatedWithAPredicate;
  private final IngestNodeSet nodesNotAssociatedWithAPredicate;

  private final SkipChain noPredicateIngestPath;

  public EvaluationPathAnalysis(IngestTree tree, PredicateAnalysis predicate) {
    this.tree = tree;
    this.predicateLeaves = collectLeaves(predicate.predicateTree());

    this.nodesAssociatedWithAPredicate = collectIngestNodesWithAPredicate(tree, predicateLeaves);
    this.nodesNotAssociatedWithAPredicate = collectIngestNodesWithoutAPredicate(tree, nodesAssociatedWithAPredicate);

    this.indexesForLeaves = calculateIndexesForLeaves(predicateLeaves);
    this.path = calculatePath(predicate.predicateTree());

    this.noPredicateIngestPath = new SkipChain(nodesNotAssociatedWithAPredicate);
  }

  private IngestNodeSet collectIngestNodesWithAPredicate(IngestTree tree, Iterable<ColumnPredicate.LeafColumnPredicate> predicateLeaves) {
    IngestNodeSet nodesAssociatedWithAPredicate = new IngestNodeSet();
    for (ColumnPredicate.LeafColumnPredicate leaf : predicateLeaves) {
      if (leaf.predicateType() != ColumnPredicateType.NONE) {
        IngestNodeSet dependenciesForThisPredicate = tree.collectIngestNodeDependenciesForPaths(leaf.columnPathString());
        nodesAssociatedWithAPredicate.addAll(dependenciesForThisPredicate);
      }
    }

    return nodesAssociatedWithAPredicate;
  }

  private IngestNodeSet collectIngestNodesWithoutAPredicate(IngestTree tree, IngestNodeSet nodesWithAPredicate) {
    // Add all the ingest-nodes from the tree.
    IngestNodeSet nodesWithoutAPredicate = new IngestNodeSet();
    for (IngestNode node : tree.ingestNodes()) {
      if (node.category() == IngestNodeCategory.DATA_INGEST) {
        nodesWithoutAPredicate.add(node);
      }
    }

    // Remove those nodes that don't have an associated predicate.
    for (IngestNode node : nodesWithAPredicate) {
      nodesWithoutAPredicate.remove(node);
    }

    return nodesWithoutAPredicate;
  }

  private List<ColumnPredicate.LeafColumnPredicate> collectLeaves(ColumnPredicate root) {
    // Pass to in-order traversal
    return calculateLeaves(root, new ArrayList<ColumnPredicate.LeafColumnPredicate>());
  }

  private List<ColumnPredicate.LeafColumnPredicate> calculateLeaves(ColumnPredicate node, List<ColumnPredicate.LeafColumnPredicate> list) {
    if (node.traversalInfo().traversalPreference() == TraversalPreference.THIS_NODE) {
      // If a node points to itself as it's preferred traversal direction, this must be a leaf.
      list.add((ColumnPredicate.LeafColumnPredicate)node);
    } else {
      // First visit the chosen-left direction, then visit the chosen-right direction.
      calculateLeaves(node.traversalInfo().chosenLhsNode(), list);
      calculateLeaves(node.traversalInfo().chosenRhsNode(), list);
    }

    return list;
  }

  private Map<ColumnPredicate.LeafColumnPredicate, Integer> calculateIndexesForLeaves(List<ColumnPredicate.LeafColumnPredicate> leaves) {
    HashMap<ColumnPredicate.LeafColumnPredicate, Integer> indexesForLeavesMap = new HashMap<ColumnPredicate.LeafColumnPredicate, Integer>();

    for (int index = 0; index < leaves.size(); index++) {
      indexesForLeavesMap.put(leaves.get(index), index);
    }

    return indexesForLeavesMap;
  }

  private PredicateTestWayPoint calculatePath(ColumnPredicate node) {
    if (node == ColumnPredicate.LeafColumnPredicate.NONE) {
      return PredicateTestWayPoint.SUCCESS;
    }

    ColumnPredicate.LeafColumnPredicate startingNode = (ColumnPredicate.LeafColumnPredicate) findFirstChildNode(node);
    return newWayPointForLeaf(startingNode);
  }



  private ColumnPredicate findFirstChildNode(ColumnPredicate node) {
    // If the traversal-preference is 'this-node', that means we're at a leaf.
    // Otherwise, we follow the preferred traversal direction.
    if (node.traversalInfo().traversalPreference() == TraversalPreference.THIS_NODE) {
      return node;
    } else {
      return findFirstChildNode(node.traversalInfo().chosenLhsNode());
    }
  }

  private PredicateTestWayPoint determineSuccessPath(ColumnPredicate.LeafColumnPredicate node) {
    return determineSuccessPath(node, null);
  }

  private PredicateTestWayPoint determineFailurePath(ColumnPredicate.LeafColumnPredicate node) {
    return determineFailurePath(node, null);
  }

  // The success path follows the parent until it finds an AND.
  // Once we find an AND, follow its chosen right and then return the first child node.
  private PredicateTestWayPoint determineSuccessPath(ColumnPredicate current, ColumnPredicate previous) {
    if (current == null) {
      //  If we get to the top without reaching an AND, then return success. We've validated that this path works.
      return PredicateTestWayPoint.SUCCESS;
    } else if (current.predicateType() == ColumnPredicateType.AND  && current.traversalInfo().chosenRhsNode() != previous) {
      // If we've reached an AND, and the Chosen-RHS node isn't the node we just
      // recursively traversed from, then find the first child of the chosen rhs path.
      ColumnPredicate.LeafColumnPredicate nextChildNode =
        (ColumnPredicate.LeafColumnPredicate) findFirstChildNode(current.traversalInfo().chosenRhsNode());

      return newWayPointForLeaf(nextChildNode);
    } else {
      return determineSuccessPath(current.parent(), current);
    }
  }

  // The failure path follows the parent until it finds an OR.
  // Once we find an OR, follow its chosen right link and then return the first child node.
  private PredicateTestWayPoint determineFailurePath(ColumnPredicate current, ColumnPredicate previous) {
    if (current == null) {
      //  If we get to the top without reaching an OR, then return failure. We've validated that this path does not work.
      return PredicateTestWayPoint.FAILURE;
    } else if (current.predicateType() == ColumnPredicateType.OR && current.traversalInfo().chosenRhsNode() != previous) {
      // If we've reached an OR, and the chosen rhs node isn't the node we just
      // traversed from (previous), then find the first child of the chosen rhs path.
      ColumnPredicate.LeafColumnPredicate nextChildNode =
        (ColumnPredicate.LeafColumnPredicate) findFirstChildNode(current.traversalInfo().chosenRhsNode());

      return newWayPointForLeaf(nextChildNode);
    } else {
      return determineFailurePath(current.parent(), current);
    }
  }

  private PredicateTestWayPoint newWayPointForLeaf(ColumnPredicate.LeafColumnPredicate forNode) {
    PredicateTestWayPoint successPath = determineSuccessPath(forNode);
    PredicateTestWayPoint failurePath = determineFailurePath(forNode);

    SkipChain successSkipChain = computeSkipSet(forNode, successPath.leafNode());
    SkipChain failureSkipChain = computeSkipSet(forNode, failurePath.leafNode());

    return new PredicateTestWayPoint(tree, forNode, successPath, failurePath, successSkipChain, failureSkipChain);
  }

  private SkipChain computeSkipSet(ColumnPredicate.LeafColumnPredicate startNode, ColumnPredicate.LeafColumnPredicate endNode) {
    int startIndex = indexesForLeaves.get(startNode) + 1;
    int endIndex = indexesForLeaves.containsKey(endNode) ? indexesForLeaves.get(endNode) : predicateLeaves.size();
    int length = endIndex - startIndex;

    ColumnPredicate.LeafColumnPredicate[] skipSet = new ColumnPredicate.LeafColumnPredicate[length];
    for (int index = 0; index < length; index++) {
      skipSet[index] = predicateLeaves.get(index);
    }

    return new SkipChain(tree, skipSet);
  }


  public PredicateTestWayPoint path() { return path; }
  public Iterable<ColumnPredicate.LeafColumnPredicate> leaves() { return this.predicateLeaves; }

  public SkipChain finalCommitIngestPath() { return this.noPredicateIngestPath; }
}
