package org.apache.parquet.parqour.ingest.plan.evaluation.waypoints;

import org.apache.parquet.parqour.ingest.plan.evaluation.skipchain.SkipChain;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.categories.PrimitiveIngestNodeBase;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;

/**
* Created by sircodesalot on 6/8/15.
*/
public final class PredicateTestWayPoint extends WayPoint {
  public static final PredicateTestWayPoint SUCCESS = new PredicateTestWayPoint("SUCCESS", WayPointCategory.SUCCESS);
  public static final PredicateTestWayPoint FAILURE = new PredicateTestWayPoint("FAILURE", WayPointCategory.FAILURE);

  private final ColumnPredicate.LeafColumnPredicate predicateLeafNode;
  private final PredicateTestWayPoint successPath;
  private final PredicateTestWayPoint failurePath;
  private final String representation;

  private final SkipChain successSkipChain;
  private final SkipChain failureSkipChain;

  private final PrimitiveIngestNodeBase[] dependentIngestNodes;

  protected PredicateTestWayPoint(String representation, WayPointCategory category) {
    super(category);
    this.predicateLeafNode = null;
    this.successPath = this;
    this.failurePath = this;
    this.representation = representation;

    this.dependentIngestNodes = new PrimitiveIngestNodeBase[0];
    this.successSkipChain = new SkipChain();
    this.failureSkipChain = new SkipChain();
  }

  public PredicateTestWayPoint(IngestTree tree, ColumnPredicate.LeafColumnPredicate predicateLeafNode, PredicateTestWayPoint successPath, PredicateTestWayPoint failurePath) {
    this(tree, predicateLeafNode, successPath, failurePath, new SkipChain(), new SkipChain());
  }

  public PredicateTestWayPoint(IngestTree tree, ColumnPredicate.LeafColumnPredicate predicateLeafNode, PredicateTestWayPoint successPath, PredicateTestWayPoint failurePath,
                               SkipChain successSkipChain, SkipChain failureSkipChain) {
    super(WayPointCategory.TEST);

    this.predicateLeafNode = predicateLeafNode;
    this.successPath = successPath;
    this.failurePath = failurePath;
    this.representation = predicateLeafNode.columnPath().toDotString();

    this.successSkipChain = successSkipChain;
    this.failureSkipChain = failureSkipChain;

    this.dependentIngestNodes = collectIngestNodeDependencies(tree);
  }

  private PrimitiveIngestNodeBase[] collectIngestNodeDependencies(IngestTree tree) {
    IngestNodeSet ingestNodeSet = tree.collectIngestNodeDependenciesForPaths(predicateLeafNode.columnPathString());
    PrimitiveIngestNodeBase[] dependentIngestNodes = new PrimitiveIngestNodeBase[ingestNodeSet.size()];

    int index = 0;
    for (IngestNode ingestNode : ingestNodeSet) {
      dependentIngestNodes[index++] = (PrimitiveIngestNodeBase)ingestNode;
    }

    return dependentIngestNodes;
  }

  public ColumnPredicate.LeafColumnPredicate leafNode() { return this.predicateLeafNode; }
  public PredicateTestWayPoint successPath() { return this.successPath; }
  public PredicateTestWayPoint failurePath() { return this.failurePath; }

  public SkipChain successSkipChain() { return this.successSkipChain; }
  public SkipChain failureSkipChain() { return this.failureSkipChain; }

  public boolean isAtCompletionPoint() {
    return this == SUCCESS || this == FAILURE;
  }

  public boolean isSuccessOrFailure() {
    return this == SUCCESS;
  }

  public boolean execute(int rowNumber) {
    // First make sure all dependencies have been read.
    for (PrimitiveIngestNodeBase dependentIngestNode : dependentIngestNodes) {
      dependentIngestNode.read(rowNumber);
    }

    // Here is where the testing would happen.
    return predicateLeafNode.test(null);
  }

  @Override
  public String toString() {
    return this.representation;
  }

}
