package org.apache.parquet.parqour.ingest.cursor.iterable.aggregation;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.cursor.GroupAggregateCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.cursor.iterators.RecordSet;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableRecordSet;
import org.apache.parquet.parqour.ingest.cursor.lookup.CursorHash;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An aggregate is a set of columns containing links to child records. For example, if we have a schema:
 * <p/>
 * group somegroup {
 * int32 first
 * int32 second
 * }
 * <p/>
 * the job of the GroupAggregate is to link results returned by the 'first' and 'second' columns.
 * In other words, rather than copying the results and returning them upstream, we instead just
 * create an artificial pointer that connects to an index on the child nodes record-set array.
 * This improves performance because we can pre-allocate lots of memory, and then just virtually
 * connect the results without the overhead of many small allocations + collections.
 */
public class GroupAggregateIterableCursor extends GroupAggregateCursor implements Iterable<Cursor> {
  public static final int NO_RELATIONSHIP = -1;

  private final int rowCount;
  private final int childColumnCount;

  private int size;
  private final int[] childColumnRowIndexes;
  private Integer[][] childNodeLinks;

  private int totalResultSetsReported;
  private final AdvanceableCursor[] childCursorsByIndex;
  private final CursorHash childCursors;

  private final boolean[] resultSetsReported;

  private final Map<String, Integer> cursorIndexes = new HashMap<String, Integer>();

  public GroupAggregateIterableCursor(String name, int childColumnCount, int totalRowCount) {
    super(name, childColumnCount, totalRowCount);

    this.childColumnCount = childColumnCount;
    this.rowCount = totalRowCount;

    this.childColumnRowIndexes = new int[childColumnCount];
    this.childNodeLinks = new Integer[childColumnCount][totalRowCount];
    this.childCursors = new CursorHash();

    this.size = 0;
    this.totalResultSetsReported = 0;
    this.resultSetsReported = new boolean[childColumnCount];

    this.childCursorsByIndex = new AdvanceableCursor[childColumnCount];
  }

  public int getChildColumnSize(int childColumnIndex) {
    return childColumnRowIndexes[childColumnIndex];
  }

  public int maxChildColumnSize() {
    return this.size;
  }

  public int sizeForChildColumn() {
    return this.rowCount;
  }

  public int childNodeCount() {
    return this.childColumnCount;
  }


  public Integer[] getlinksForChild(int index) {
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

  @Deprecated
  public void setResultSetForChildIndex(int childColumnIndex, Integer[] items) {
    this.childNodeLinks[childColumnIndex] = items;
  }

  public void setChildCursor(int childColumnIndex, AdvanceableCursor cursor) {
    this.childCursors.add(cursor);
    this.childCursorsByIndex[childColumnIndex] = cursor;
    this.cursorIndexes.put(cursor.name(), childColumnIndex);
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

  public int getLinkForChild(int childColumnIndex, int rowIndex) {
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
    int index = this.cursorIndexes.get(path);
    if (childNodeLinks[index][start] != null) {
      return childCursorsByIndex[index];
    } else {
      return null;
    }
  }


  @Override
  public RollableRecordSet<Integer> i32Iter(String path) {
    int index = this.cursorIndexes.get(path);
    Integer startOffset = childNodeLinks[index][start];

    if (startOffset != null) {
      return childCursorsByIndex[index].i32StartIteration(startOffset);
    } else {
      return RollableRecordSet.EMPTY_I32_RECORDSET;
    }
  }

  @Override
  public RollableRecordSet<Integer> i32Iter(int nodeIndex) {
    int start = childNodeLinks[nodeIndex][this.start];
    int end = childNodeLinks[nodeIndex][this.start + 1];

    childCursorsByIndex[nodeIndex].setRange(start, end);
    return childCursorsByIndex[nodeIndex].i32Iter();
  }

  @Override
  public RecordSet<Cursor> fieldIter(int nodeIndex) {
    int start = childNodeLinks[nodeIndex][this.start];
    int end = childNodeLinks[nodeIndex][this.start + 1];

    childCursorsByIndex[nodeIndex].setRange(start, end);
    return childCursorsByIndex[nodeIndex].fieldIter();
  }

  @Override
  public RecordSet<Cursor> fieldIter(String path) {
    int index = this.cursorIndexes.get(path);
    Integer startOffset = childNodeLinks[index][start];

    if (startOffset != null) {
      return childCursorsByIndex[index].fieldStartIteration(startOffset);
    } else {
      return RollableRecordSet.EMPTY_CURSOR_RECORDSET;
    }
  }

  @Override
  public RecordSet<Cursor> fieldStartIteration(int startOffset) {
    this.iterator = new GroupCursorIterator(getlinksForChild(0));
    this.iterator.reset(startOffset);
    return new RecordSet<Cursor>(this);
  }

  private GroupCursorIterator iterator;

  @Override
  public Iterator<Cursor> iterator() {
    return this.iterator();
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
    Integer[] linksForIndex = childNodeLinks[index];
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
