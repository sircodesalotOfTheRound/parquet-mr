package org.apache.parquet.parqour.ingest.cursor;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableRecordSet;
import org.apache.parquet.parqour.ingest.cursor.iterators.RecordSet;
import org.apache.parquet.parqour.ingest.cursor.lookup.CursorHash;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * An aggregate is a set of columns containing links to child records. For example, if we have a schema:
 *
 *   group somegroup {
 *     int32 first
 *     int32 second
 *   }
 *
 *   the job of the GroupAggregate is to link results returned by the 'first' and 'second' columns.
 *   In other words, rather than copying the results and returning them upstream, we instead just
 *   create an artificial pointer that connects to an index on the child nodes record-set array.
 *   This improves performance because we can pre-allocate lots of memory, and then just virtually
 *   connect the results without the overhead of many small allocations + collections.
 */
public class GroupAggregateCursor extends AdvanceableCursor implements Iterable<Cursor> {
  public static final int NO_RELATIONSHIP = -1;

  private final int rowCount;
  private final int childColumnCount;

  private int size;
  private final int[] childColumnRowIndexes;
  private int[][] childNodeLinks;

  private int totalResultSetsReported;
  private final AdvanceableCursor[] childCursorsByIndex;
  private final CursorHash childCursors;

  private final boolean[] resultSetsReported;

  public GroupAggregateCursor(String name, int childColumnCount, int totalRowCount) {
    super(name);

    this.childColumnCount = childColumnCount;
    this.rowCount = totalRowCount;

    this.childColumnRowIndexes = new int[childColumnCount];
    this.childNodeLinks = new int[childColumnCount][totalRowCount];
    this.childCursors = new CursorHash();

    this.size = 0;
    this.totalResultSetsReported = 0;
    this.resultSetsReported = new boolean[childColumnCount];

    this.childCursorsByIndex = new AdvanceableCursor[childColumnCount];
  }

  public int getChildColumnSize(int childColumnIndex) {
    return childColumnRowIndexes[childColumnIndex];
  }

  public int maxChildColumnSize() { return this.size; }
  public int sizeForChildColumn() { return this.rowCount; }
  public int childNodeCount() { return this.childColumnCount; }


  public int[] getlinksForChild(int index) {
    return childNodeLinks[index];
  }

  // Returns true if a new record for this column has been added.
  public void setRelationship(int childColumnIndex, int childRecordIndex) {
    int rowIndex = childColumnRowIndexes[childColumnIndex]++;
    childNodeLinks[childColumnIndex][rowIndex] = childRecordIndex;

    // If this column has the most rows, then update the maximum size.
    // Also, this indicates that a new entry for this row has been created.
    if (rowIndex >= size) {
      size++;
    }
  }

  public void setResultSetForChildIndex(int childColumnIndex, int[] items) {
    this.childNodeLinks[childColumnIndex] = items;
  }

  public void setChildCursor(int childColumnIndex, AdvanceableCursor cursor) {
    this.childCursors.add(cursor);
    this.childCursorsByIndex[childColumnIndex] = cursor;
  }

  public void setResultsReported(int childColumnIndex) {
    if (resultSetsReported[childColumnIndex]) {
      throw new DataIngestException("This results for this column have already been set.");
    }

    this.resultSetsReported[childColumnIndex] = true;
    this.totalResultSetsReported++;
  }

  public boolean allResultsReported() {
    return this.totalResultSetsReported == childCursorsByIndex.length;
  }

  public int getLinkForChild(int childColumnIndex, int rowIndex)  {
    return childNodeLinks[childColumnIndex][rowIndex];
  }

  public <T extends Cursor> T getResultSetForColumn(int childColumnIndex) {
    return (T) childCursorsByIndex[childColumnIndex];
  }

  //////////////////////////////////
  // Cursor actions:
  //////////////////////////////////

  @Override
  public Cursor field(String path) {
    return childCursors.get(path);
  }




  @Override
  public RollableRecordSet<Integer> i32iter(int nodeIndex) {
    int start = childNodeLinks[nodeIndex][this.start];
    int end = childNodeLinks[nodeIndex][this.start + 1];

    childCursorsByIndex[nodeIndex].setRange(start, end);
    return childCursorsByIndex[nodeIndex].i32iter();
  }

  public RecordSet<Cursor> fieldIter(int nodeIndex) {
    int start = childNodeLinks[nodeIndex][this.start];
    int end = childNodeLinks[nodeIndex][this.start + 1];

    childCursorsByIndex[nodeIndex].setRange(start, end);
    return childCursorsByIndex[nodeIndex].fieldIter();
  }

  @Override
  public Iterator<Cursor> iterator() {
    return new FieldIterator(this, start, end);
  }

  private static class FieldIterator implements Iterator<Cursor> {
    private final AdvanceableCursor cursor;
    private final int start;
    private final int end;

    private int index;
    public FieldIterator(AdvanceableCursor cursor, int start, int end) {
      this.cursor = cursor;
      this.start = start;
      this.end = end;

      this.index = start;
    }

    @Override
    public boolean hasNext() {
      return index < end;
    }

    @Override
    public Cursor next() {
      cursor.setRange(index, index + 1);
      index++;
      return cursor;
    }

    @Override
    public void remove() {
      throw new NotImplementedException();
    }
  }

  @Override
  public RecordSet<Cursor> fieldIter() {
    return new RecordSet<Cursor>(this);
  }

  @Override
  public Integer i32(String path) {
    return childCursors.get(path).i32();
  }

  @Override
  public Integer i32(int index) {
    return childCursorsByIndex[index].i32();
  }

  @Override
  public Object value(String path) {
    return childCursors.get(path).value();
  }

  @Override
  public Cursor field(int index) {
    int[] linksForIndex = childNodeLinks[index];
    if (linksForIndex[start] != linksForIndex[start + 1]) {
      return childCursorsByIndex[index];
    } else {
      return null;
    }
  }

  @Override
  public Object value() {
    return this;
  }

  public void clear() {
    for (int index = 0; index < childColumnCount; index++) {
      this.childColumnRowIndexes[index] = 0;
    }

    for (int index = 0; index < childColumnCount; index++) {
      this.resultSetsReported[index] = false;
    }

    this.size = 0;
    this.totalResultSetsReported = 0;
  }

}
