package org.apache.parquet.parqour.ingest.cursor.iterable.field;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;

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
    int columnIndex = cursorIndexes.get(path);
    int schemaLinkToChild = schemaLinks[columnIndex][index];
    return childCursors[columnIndex].advanceTo(schemaLinkToChild).i32();
  }
}
