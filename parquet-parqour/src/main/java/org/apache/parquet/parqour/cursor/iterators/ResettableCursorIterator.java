package org.apache.parquet.parqour.cursor.iterators;

import java.util.Iterator;

/**
 * Created by sircodesalot on 7/13/15.
 */
public abstract class ResettableCursorIterator<T> implements Iterator<T> {
  public abstract void reset(int start);
}
