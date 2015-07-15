package org.apache.parquet.parqour.ingest.cursor.iterable.field;

import org.apache.parquet.parqour.ingest.cursor.GroupAggregateCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.cursor.iterators.RecordSet;
import org.apache.parquet.parqour.ingest.cursor.lookup.CursorHash;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An aggregate is a set of columns containing links to child records. For example, if we have a schema:
 * <p/>
 * group somegroup {
 *   int32 first
 *   int32 second
 * }
 * <p/>
 * the job of the GroupAggregate is to link results returned by the 'first' and 'second' columns.
 * In other words, rather than copying the results and returning them upstream, we instead just
 * create an artificial pointer that connects to an index on the child nodes record-set array.
 * This improves performance because we can pre-allocate lots of memory, and then just virtually
 * connect the results without the overhead of many small allocations + collections.
 */
public class GroupAggregateIterableCursor extends GroupAggregateCursor implements Iterable<Cursor> {
  private final int fieldCount;
  private Integer[][] schemaLinks;

  private final AdvanceableCursor[] childCursorsByIndex;
  private final CursorHash childCursors;


  private final Map<String, Integer> cursorIndexes = new HashMap<String, Integer>();

  public GroupAggregateIterableCursor(String name, Integer[][] schemaLinks) {
    super(name, schemaLinks);

    this.fieldCount = schemaLinks.length;
    this.schemaLinks = schemaLinks;
    this.childCursors = new CursorHash();

    this.childCursorsByIndex = new AdvanceableCursor[fieldCount];
  }

  public Integer[] getlinksForChild(int index) {
    return schemaLinks[index];
  }

  @Deprecated
  public void setResultSetForChildIndex(int childColumnIndex, Integer[] items) {
    this.schemaLinks[childColumnIndex] = items;
  }

  public void setChildCursor(int childColumnIndex, AdvanceableCursor cursor) {
    this.childCursors.add(cursor);
    this.childCursorsByIndex[childColumnIndex] = cursor;
    this.cursorIndexes.put(cursor.name(), childColumnIndex);
  }

  //////////////////////////////////
  // Cursor actions:
  //////////////////////////////////


  @Override
  public RecordSet<Cursor> fieldStartIteration(int columnIndex, int startOffset) {
    this.iterator = new GroupCursorIterator(this, getlinksForChild(columnIndex));
    this.iterator.reset(startOffset);
    return new RecordSet<Cursor>(this);
  }

  private GroupCursorIterator iterator;

  @Override
  public Iterator<Cursor> iterator() {
    return this.iterator;
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
}
