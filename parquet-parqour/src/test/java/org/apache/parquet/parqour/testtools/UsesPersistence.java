package org.apache.parquet.parqour.testtools;

import org.junit.After;

/**
 * Created by sircodesalot on 6/15/15.
 */
public abstract class UsesPersistence {
  private final boolean deleteDataOnExit;

  public UsesPersistence() { this(true); }
  public UsesPersistence(boolean deleteDataOnExit) {
    this.deleteDataOnExit = deleteDataOnExit;
  }

  @After
  public void deleteTestData() {
    if (deleteDataOnExit) {
      TestTools.deleteTestData();
    }
  }
}
