package org.apache.parquet.parqour.ingest.plan.evaluation.skipchain;

import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.SkipChainWayPoint;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.WayPoint;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.categories.ColumnIngestNodeBase;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;

/**
 * Created by sircodesalot on 6/21/15.
 */
public class SkipChain {
  private SkipChainWayPoint originalBegin;
  private SkipChainWayPoint originalEnd;

  private SkipChainWayPoint begin;
  private SkipChainWayPoint end;

  public SkipChain() {
    this.begin = null;
    this.end = null;

    this.originalBegin = null;
    this.originalEnd = null;
  }

  public SkipChain(Iterable<IngestNode> ingestNodes) {
    for (IngestNode ingestNode : ingestNodes) {
      SkipChainWayPoint wayPoint = new SkipChainWayPoint((ColumnIngestNodeBase) ingestNode);
      if (begin == null) {
        begin = end = wayPoint;
      } else {
        end.setNext(wayPoint);
        end = end.next();
      }
    }

    this.originalBegin = begin;
    this.originalEnd = end;
  }

  @Deprecated // Replace with iterable
  public SkipChain(IngestTree tree, ColumnPredicate.LeafColumnPredicate[] predicates) {
    for (ColumnPredicate.LeafColumnPredicate predicate : predicates) {
      SkipChainWayPoint wayPoint = new SkipChainWayPoint(tree, predicate);
      if (begin == null) {
        begin = end = wayPoint;
      } else {
        end.setNext(wayPoint);
        end = end.next();
      }
    }

    this.originalBegin = begin;
    this.originalEnd = end;
  }

  public SkipChain append(SkipChain chain) {
    // If the chain is empty, do nothing.
    if (chain.begin == null) {
      return this;
    }

    if (this.end == null) {
      this.begin = this.end = chain.begin;
    } else {
      this.end.setNext(chain.begin);
      this.end = chain.end;
    }

    return this;
  }

  public SkipChain reset() {
    this.begin = originalBegin;
    this.end = originalEnd;

    if (this.end != null) {
      this.end.setNext(null);
    }

    return this;
  }

  public WayPoint path() {
    return this.begin;
  }
}
