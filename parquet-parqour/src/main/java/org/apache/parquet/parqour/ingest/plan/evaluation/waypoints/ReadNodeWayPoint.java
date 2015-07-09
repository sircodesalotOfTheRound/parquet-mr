package org.apache.parquet.parqour.ingest.plan.evaluation.waypoints;

/**
 * Created by sircodesalot on 6/22/15.
 */
public class ReadNodeWayPoint extends WayPoint {

  protected ReadNodeWayPoint() {
    super(WayPointCategory.READ);
  }

  @Override
  public WayPoint successPath() {
    return null;
  }

  @Override
  public WayPoint failurePath() {
    return null;
  }

  @Override
  public boolean execute(int rowNumber) {
    return false;
  }
}
