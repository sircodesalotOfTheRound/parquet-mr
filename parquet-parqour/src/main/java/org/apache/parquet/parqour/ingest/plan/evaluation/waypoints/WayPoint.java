package org.apache.parquet.parqour.ingest.plan.evaluation.waypoints;

/**
 * Created by sircodesalot on 6/21/15.
 */
public abstract class WayPoint {
  private final WayPointCategory category;

  protected WayPoint(WayPointCategory category) {
    this.category = category;
  }

  public abstract WayPoint successPath();
  public abstract WayPoint failurePath();
  public abstract boolean execute(int rowNumber);

  public WayPointCategory category() { return this.category; }
}
