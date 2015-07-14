package org.apache.parquet.parqour.ingest.read.driver;

import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.PredicateTestWayPoint;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.WayPoint;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class ParqourPredicateReadDriver extends ParqourReadDriver {
  private final PredicateTestWayPoint predicatePathStartPoint;

  public ParqourPredicateReadDriver(SchemaInfo schemaInfo) {
    super(schemaInfo);

    this.predicatePathStartPoint = super.pathAnalysis.path();
  }
  @Override
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
