package org.apache.parquet.parqour.ingest.cursor;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.cursor.lookup.CursorHash;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;

import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by sircodesalot on 6/18/15.
 */
public class TestCursorHash {
  private static final int SIZE = TestTools.generateRandomInt(1000);
  @Test
  public void testCursorHash() {
    HashMap<String, Cursor> normalCursorMap = new HashMap<String, Cursor>();
    CursorHash cursorHash = new CursorHash();

    System.out.println(SIZE);
    for (int index = 0; index < SIZE; index++) {
      Integer randomNumber = TestTools.generateRandomInt(1000000);
      String randomString = randomNumber.toString();

      // Mock cursor with a random string name.
      AdvanceableCursor mockCursor = mock(AdvanceableCursor.class);
      when(mockCursor.name()).thenReturn(randomString);

      if (!normalCursorMap.containsKey(randomString)) {
        normalCursorMap.put(randomString, mockCursor);
        cursorHash.add(mockCursor);
      }
    }

    // Performance comes from matching the same string object more than once.
    for (int index = 0; index < 10000; index++) {
      for (String key : normalCursorMap.keySet()) {
        Cursor cursorFromNormalMap = normalCursorMap.get(key);
        Cursor cursorFromHash = cursorHash.get(key);

        assert (cursorFromNormalMap == cursorFromHash);
      }
    }
  }
}
