package org.apache.parquet.parqour.ingest.read.driver;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.plan.analysis.PredicateAnalysis;
import org.apache.parquet.parqour.ingest.plan.evaluation.EvaluationPathAnalysis;
import org.apache.parquet.parqour.ingest.plan.evaluation.skipchain.SkipChain;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.PredicateTestWayPoint;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.WayPoint;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class ParqourReadDriver {
  private final DiskInterfaceManager diskInterfaceManager;
  private final IngestTree ingestTree;
  private final PredicateAnalysis predicateAnalysis;
  private final EvaluationPathAnalysis pathAnalysis;

  private final SkipChain finalCommitIngestPath; // The nodes not associates with a predicate.
  private final PredicateTestWayPoint predicatePathStartPoint;
  private final long rowCount;

  private long rowNumber;

  public ParqourReadDriver(SchemaInfo schemaInfo) {
    this.diskInterfaceManager = new DiskInterfaceManager(schemaInfo);
    this.ingestTree = new IngestTree(schemaInfo, diskInterfaceManager);
    this.predicateAnalysis = new PredicateAnalysis(ingestTree);
    this.pathAnalysis = new EvaluationPathAnalysis(ingestTree, predicateAnalysis);

    this.finalCommitIngestPath = pathAnalysis.finalCommitIngestPath();
    this.predicatePathStartPoint = pathAnalysis.path();

    this.rowCount = schemaInfo.totalRowCount();
    this.rowNumber = -1;
  }

  public Cursor cursor() {
   return ingestTree.root().collectAggregate();
  }

  public boolean readNext() {
    while (++rowNumber < rowCount) {
      finalCommitIngestPath.reset();
      if (testPredicates((int) rowNumber)) {
        performFinalCommit();
        return true;
      }
    }

    return false;
  }

  public boolean testPredicates(int rowNumber) {
    PredicateTestWayPoint current = this.predicatePathStartPoint;
    while (!current.isAtCompletionPoint()) {
      if (current.execute(rowNumber)) {
        finalCommitIngestPath.append(current.successSkipChain());
        current = current.successPath();
      } else {
        finalCommitIngestPath.append(current.failureSkipChain());
        current = current.failurePath();
      }
    }

    return current.isSuccessOrFailure();
  }

  public void performFinalCommit() {
    WayPoint current = finalCommitIngestPath.path();
    while (current != null) {
      current.execute((int) rowNumber);
      current = current.successPath();
    }
  }
}
