package org.apache.parquet.parqour.ingest.plan.evaluation;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNodeCategory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 6/17/15.
 */
@Deprecated
public class IngestNodePartition {
  private final IngestNodeSet ingestNodesWithPredicates;
  private final IngestNodeSet ingestNodesWithoutPredicates;

  public IngestNodePartition(IngestTree tree, Iterable<ColumnPredicate.LeafColumnPredicate> predicateLeaves) {
    this.ingestNodesWithPredicates = collectIngestNodesWithPredicates(tree, predicateLeaves);
    this.ingestNodesWithoutPredicates = collectIngestNodesWithoutPredicates(tree, ingestNodesWithPredicates);
  }

  private IngestNodeSet collectIngestNodesWithPredicates(IngestTree tree, Iterable<ColumnPredicate.LeafColumnPredicate> predicateLeaves) {
    List<IngestNode> ingestNodesWithPredicates = new ArrayList<IngestNode>();
    for (ColumnPredicate.LeafColumnPredicate leaf : predicateLeaves) {
      if (leaf.predicateType() != ColumnPredicateType.NONE) {
        IngestNode ingestNode = tree.getIngestNodeByPath(leaf.columnPathString());
        ingestNodesWithPredicates.add(ingestNode);

        // If a parent 'aggregating' node has a predicate, then the children
        // nodes are considered to have a predicate as well. Function returns immediately
        // if the node is a 'data ingest' or column leaf node.
        addChildrenToList(ingestNode, ingestNodesWithPredicates);
      }
    }

    return new IngestNodeSet(ingestNodesWithPredicates);
  }

  private List<IngestNode> addChildrenToList(IngestNode node, List<IngestNode> list) {
    if (node.category() == IngestNodeCategory.DATA_INGEST) {
      return list;
    }

    // Pre-order tree descent, add all children if this is an aggregating node.
    AggregatingIngestNode aggregator = (AggregatingIngestNode)node;
    for (IngestNode child : aggregator.children()) {
      list.add(child);
      addChildrenToList(child, list);
    }

    return list;
  }

  private IngestNodeSet collectIngestNodesWithoutPredicates(IngestTree tree, IngestNodeSet ingestNodesWithPredicates) {
    List<IngestNode> nodesWithoutPredicates = new ArrayList<IngestNode>();
    for (IngestNode ingestNode : tree.ingestNodes()) {
      if (!ingestNodesWithPredicates.containsPath(ingestNode.path()) && ingestNode.category() == IngestNodeCategory.DATA_INGEST) {
        nodesWithoutPredicates.add(ingestNode);
      }
    }

    return new IngestNodeSet(nodesWithoutPredicates);
  }

  @Deprecated
  public IngestNodeSet ingestNodesWithPredicates() { return this.ingestNodesWithPredicates; }

  @Deprecated
  public IngestNodeSet ingestNodesWithoutPredicates() { return this.ingestNodesWithoutPredicates; }
}
