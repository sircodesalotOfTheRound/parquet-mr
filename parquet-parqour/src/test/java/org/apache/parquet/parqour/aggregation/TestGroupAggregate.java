package org.apache.parquet.parqour.aggregation;

import org.apache.parquet.parqour.ingest.cursor.GroupAggregateCursor;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/16/15.
 */
public class TestGroupAggregate {
  private static int TOTAL_SIZE = TestTools.generateRandomInt(1000000);

  @Test
  public void testColumnAggregateList() {
    int numberOfChildColumns = 3;
    int numberOfRows = TOTAL_SIZE;
    GroupAggregateCursor aggregate = new GroupAggregateCursor("simple_cursor", numberOfChildColumns, numberOfRows);

    // Write to each of the three columns, but do so out of order.
    for (int columnIndex : new int[] { 1, 0, 2 }) {
      for (int rowIndex = 0; rowIndex < TOTAL_SIZE; rowIndex++) {
        // Multiply by 1, 2, or 3, depending on which column we're writing to.
        // So column 1 is 1x, column 2 is 2x, and column 3 is 3x.
        //
        // We can interpret this as meaning that the first column updates its
        // indexes every row (like something with a REQUIRED definition-level),
        // wheras the second is REPEATED with two items, and column 3 is REPEATED
        // with 3 items.
        int multiplier = (columnIndex + 1) * rowIndex;
        aggregate.setRelationship(columnIndex, multiplier);
      }
    }

    for (int rowIndex = 0; rowIndex < aggregate.maxChildColumnSize(); rowIndex++) {
      for (int columnIndex = 0; columnIndex < numberOfChildColumns; columnIndex++) {
        if (rowIndex < aggregate.getChildColumnSize(columnIndex)) {
          int multiplier = (columnIndex + 1) * rowIndex;
          assertEquals(multiplier, aggregate.getLinkForChild(columnIndex, rowIndex));
        }
      }
    }
  }
}
