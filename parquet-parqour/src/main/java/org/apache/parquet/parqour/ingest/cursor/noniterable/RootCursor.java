package org.apache.parquet.parqour.ingest.cursor.noniterable;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.recordsets.FieldEntries;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableFieldEntries;
import org.apache.parquet.parqour.ingest.cursor.lookup.CursorHash;

import java.util.HashMap;
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
public class RootCursor extends AdvanceableCursor {
  private static final String ROOT_NAME = "root";
  private static final int ROOT_COLUMN = 0;

  private Integer[] schemaLinks;

  private final AdvanceableCursor[] childCursorsByIndex;
  private final CursorHash childCursors;

  private final Map<String, Integer> cursorIndexes = new HashMap<String, Integer>();

  public RootCursor(AdvanceableCursor[] childCursors, Integer[] schemaLinks) {
    super(ROOT_NAME, ROOT_COLUMN);

    this.schemaLinks = schemaLinks;
    this.childCursors = new CursorHash();

    this.childCursorsByIndex = applyChildCursors(childCursors);
  }

  private AdvanceableCursor[] applyChildCursors(AdvanceableCursor[] childCursors) {
    for (AdvanceableCursor childCursor : childCursors) {
      this.childCursors.add(childCursor);
      this.cursorIndexes.put(childCursor.name(), childCursor.columnIndex());
    }

    return childCursors;
  }

  @Override
  public FieldEntries<Cursor> fieldIter(int columnIndex) {
    Integer startOffset = schemaLinks[columnIndex];

    if (startOffset != null) {
      return childCursorsByIndex[columnIndex].fieldStartIteration(columnIndex, startOffset);
    } else {
      return RollableFieldEntries.EMPTY_CURSOR_RECORDSET;
    }
  }

  @Override
  public FieldEntries<Cursor> fieldIter(String path) {
    int columnIndex = this.cursorIndexes.get(path);
    Integer startOffset = schemaLinks[columnIndex];

    if (startOffset != null) {
      return childCursorsByIndex[columnIndex].fieldStartIteration(columnIndex, startOffset);
    } else {
      return RollableFieldEntries.EMPTY_CURSOR_RECORDSET;
    }
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
  public RollableFieldEntries<Integer> i32Iter(int columnIndex) {
    Integer startOffset = schemaLinks[columnIndex];

    if (startOffset != null) {
      return childCursorsByIndex[columnIndex].i32StartIteration(startOffset);
    } else {
      return RollableFieldEntries.EMPTY_I32_RECORDSET;
    }
  }

  @Override
  public RollableFieldEntries<Integer> i32Iter(String path) {
    int columnIndex = this.cursorIndexes.get(path);
    Integer startOffset = schemaLinks[columnIndex];

    if (startOffset != null) {
      return childCursorsByIndex[columnIndex].i32StartIteration(startOffset);
    } else {
      return RollableFieldEntries.EMPTY_I32_RECORDSET;
    }
  }


  @Override
  public Cursor field(int columnIndex) {
    if (schemaLinks[columnIndex] != null) {
      return childCursorsByIndex[columnIndex];
    } else {
      return null;
    }
  }

  @Override
  public Cursor field(String path) {
    int columnIndex = this.cursorIndexes.get(path);
    if (schemaLinks[columnIndex] != null) {
      return childCursorsByIndex[columnIndex];
    } else {
      return null;
    }
  }

  @Override
  public Object value(int columnIndex) {
    return childCursorsByIndex[columnIndex].value();
  }

  @Override
  public Object value(String path) {
    return childCursors.get(path).value();
  }

  @Override
  public Object value() {
    return this;
  }
}
