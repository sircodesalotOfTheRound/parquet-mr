package org.apache.parquet.parqour.ingest.read.iterator.lamba;

/**
 * Created by sircodesalot on 6/27/15.
 */
public interface Predicate<T> {
  boolean test(T object);
}
