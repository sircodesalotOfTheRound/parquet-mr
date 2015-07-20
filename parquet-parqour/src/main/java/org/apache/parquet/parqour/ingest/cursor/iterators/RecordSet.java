package org.apache.parquet.parqour.ingest.cursor.iterators;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;

import java.util.*;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class RecordSet<T> implements Iterable<T> {
  private final Iterable<T> iterable;

  public RecordSet() {
    this.iterable = generateEmptyIterable();
  }

  private Iterable<T> generateEmptyIterable() {
    final Iterator<T> emptyIterator = new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public T next() {
        return null;
      }
    };

    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        return emptyIterator;
      }
    };
  }

  public RecordSet(Iterable<T> iterable) {
    this.iterable = iterable;
  }

  public final RecordSet<T> filter(Predicate<T> predicate) {
    return new RecordsetFilterIterable<T>(this, predicate);
  }

  public final <U extends Comparable<U>> RecordSet<T> sortBy(final Projection<T, U> onProperty) {
    List<T> collection = new ArrayList<T>();
    for (T item : this) {
      collection.add(item);
    }

    Collections.sort(collection, new Comparator<T>() {
      @Override
      public int compare(T left, T right) {
        U lhs = onProperty.apply(left);
        U rhs = onProperty.apply(right);
        return lhs.compareTo(rhs);
      }
    });

    return new RecordSet<T>(collection);
  }

  public final <U> U reduce(U aggregate, Reducer<T, U> reducer) {
    for (T item : this) {
      aggregate = reducer.nextItem(aggregate, item);
    }

    return aggregate;
  }

  public final <U> Iterable<U> roll(Projection<T, U> projection) {
    List<U> items = new ArrayList<U>();
    for (T item : this) {
      items.add(projection.apply(item));
    }

    return items;
  }

  @Override
  public Iterator<T> iterator() {
    return iterable.iterator();
  }
}
