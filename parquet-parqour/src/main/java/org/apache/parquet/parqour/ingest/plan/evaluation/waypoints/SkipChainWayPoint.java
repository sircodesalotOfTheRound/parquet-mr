package org.apache.parquet.parqour.ingest.plan.evaluation.waypoints;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.categories.ColumnIngestNodeBase;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;

/**
 * Created by sircodesalot on 6/21/15.
 */
public class SkipChainWayPoint extends WayPoint {
  private final ColumnIngestNodeBase ingestNode;
  private SkipChainWayPoint next;

  public SkipChainWayPoint(ColumnIngestNodeBase ingestNode) {
    super (WayPointCategory.READ);

    this.ingestNode = ingestNode;
    this.next = null;
  }

  public SkipChainWayPoint(IngestTree tree, ColumnPredicate.LeafColumnPredicate leafNode) {
    super(WayPointCategory.READ);

    this.ingestNode = (ColumnIngestNodeBase)tree.getIngestNodeByPath(leafNode.columnPathString());
    this.next = null;
  }

  @Override
  public WayPoint successPath() {
    return this.next;
  }

  @Override
  public WayPoint failurePath() {
    return this.next;
  }

  public SkipChainWayPoint next() {
    return this.next;
  }

  public IngestNode ingestNode() { return this.ingestNode; }

  @Override
  public boolean execute(int rowNumber) {
    ingestNode.read(rowNumber);

    return true;
  }

  public void setNext(SkipChainWayPoint next) {
    this.next = next;
  }
}
