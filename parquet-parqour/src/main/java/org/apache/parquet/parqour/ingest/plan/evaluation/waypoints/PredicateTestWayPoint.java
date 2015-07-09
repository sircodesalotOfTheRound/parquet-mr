package org.apache.parquet.parqour.ingest.plan.evaluation.waypoints;

import org.apache.parquet.parqour.ingest.plan.evaluation.skipchain.SkipChain;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.categories.ColumnIngestNodeBase;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;

/**
* Created by sircodesalot on 6/8/15.
*/
public final class PredicateTestWayPoint extends WayPoint {
  public static final PredicateTestWayPoint SUCCESS = new PredicateTestWayPoint("SUCCESS", WayPointCategory.SUCCESS);
  public static final PredicateTestWayPoint FAILURE = new PredicateTestWayPoint("FAILURE", WayPointCategory.FAILURE);

  private final ColumnPredicate.LeafColumnPredicate leafNode;
  private final PredicateTestWayPoint successPath;
  private final PredicateTestWayPoint failurePath;
  private final String representation;

  private final SkipChain successSkipChain;
  private final SkipChain failureSkipChain;

  private final IngestNodeSet dependentIngestNodes;

  protected PredicateTestWayPoint(String representation, WayPointCategory category) {
    super(category);
    this.leafNode = null;
    this.successPath = this;
    this.failurePath = this;
    this.representation = representation;

    this.dependentIngestNodes = new IngestNodeSet();
    this.successSkipChain = new SkipChain();
    this.failureSkipChain = new SkipChain();
  }

  public PredicateTestWayPoint(IngestTree tree, ColumnPredicate.LeafColumnPredicate leafNode, PredicateTestWayPoint successPath, PredicateTestWayPoint failurePath) {
    this(tree, leafNode, successPath, failurePath, new SkipChain(), new SkipChain());
  }

  public PredicateTestWayPoint(IngestTree tree, ColumnPredicate.LeafColumnPredicate leafNode, PredicateTestWayPoint successPath, PredicateTestWayPoint failurePath,
                               SkipChain successSkipChain, SkipChain failureSkipChain) {
    super(WayPointCategory.TEST);

    this.leafNode = leafNode;
    this.successPath = successPath;
    this.failurePath = failurePath;
    this.representation = leafNode.columnPath().toDotString();

    this.successSkipChain = successSkipChain;
    this.failureSkipChain = failureSkipChain;

    this.dependentIngestNodes = tree.collectIngestNodeDependenciesForPaths(leafNode.columnPathString());
  }

  public ColumnPredicate.LeafColumnPredicate leafNode() { return this.leafNode; }
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
    for (IngestNode dependency : dependentIngestNodes) {
      // Todo: remove this cast.
      ((ColumnIngestNodeBase)dependency).read(rowNumber);
    }

    // Here is where the testing would happen.
    return leafNode.test(null);
  }

  @Override
  public String toString() {
    return this.representation;
  }

}
