package org.apache.parquet.parqour.ingest.cursor.implementations.iterable.field;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableFieldEntries;
import org.apache.parquet.parqour.ingest.entrysets.FieldEntries;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sircodesalot on 7/21/15.
 */
public class GroupIterIndexingCursor extends AdvanceableCursor {
  private static final int ZERO_INDEX = 0;
  private final Integer[][] schemaLinks;
  private final AdvanceableCursor[] childCursors;
  private final Map<String, Integer> cursorIndexes = new HashMap<String, Integer>();

  public GroupIterIndexingCursor(String name, AdvanceableCursor[] childCursors, Integer[][] schemaLinks) {
    super(name, ZERO_INDEX);

    this.schemaLinks = schemaLinks;
    this.childCursors = applyChildCursors(childCursors);
  }

  private AdvanceableCursor[] applyChildCursors(AdvanceableCursor[] childCursors) {
    for (AdvanceableCursor childCursor : childCursors) {
      this.cursorIndexes.put(childCursor.name(), childCursor.columnIndex());
    }

    return childCursors;
  }

  @Override
  public Integer i32(String path) {
    // TODO: Likely this will be called repeatedly by the same string. Look into optimizations.
    int columnIndex = cursorIndexes.get(path);
    int schemaLinkToChild = schemaLinks[columnIndex][index];
    return childCursors[columnIndex].advanceTo(schemaLinkToChild).i32();
  }

  @Override
  public RollableFieldEntries<Integer> i32Iter(String path) {
    int columnIndex = cursorIndexes.get(path);
    Integer schemaLinkToChild = schemaLinks[columnIndex][index];
    if (schemaLinkToChild != null) {
      return childCursors[columnIndex].i32StartIteration(schemaLinkToChild);
    } else {
      return RollableFieldEntries.EMPTY_I32_RECORDSET;
    }
  }


  @Override
  public FieldEntries<Cursor> fieldIter(String path) {
    int columnIndex = this.cursorIndexes.get(path);
    Integer startOffset = schemaLinks[columnIndex][index];

    if (startOffset != null) {
      return childCursors[columnIndex].fieldStartIteration(columnIndex, startOffset);
    } else {
      return RollableFieldEntries.EMPTY_CURSOR_RECORDSET;
    }
  }
}
