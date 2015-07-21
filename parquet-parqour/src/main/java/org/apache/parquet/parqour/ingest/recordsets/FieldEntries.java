package org.apache.parquet.parqour.ingest.recordsets;

import org.apache.parquet.parqour.ingest.cursor.collections.Roll;
import org.apache.parquet.parqour.ingest.recordsets.transforms.FieldEntryProjectionTransform;
import org.apache.parquet.parqour.ingest.recordsets.transforms.FieldEntryReducerTransform;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.ingest.recordsets.transforms.FieldEntryFilterTransform;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class FieldEntries<T> implements Iterable<T> {
  private final Iterable<T> iterable;

  public FieldEntries() {
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

  public FieldEntries(Iterable<T> iterable) {
    this.iterable = iterable;
  }

  public final FieldEntries<T> filter(Predicate<T> predicate) {
    return new FieldEntryFilterTransform<T>(this, predicate);
  }

  public final <U extends Comparable<U>> FieldEntries<T> sortBy(final Projection<T, U> onProperty) {
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

    return new FieldEntries<T>(collection);
  }

  public final <U> U reduce(U aggregate, FieldEntryReducerTransform<T, U> reducer) {
    for (T item : this) {
      aggregate = reducer.nextItem(aggregate, item);
    }

    return aggregate;
  }

  public final <U> FieldEntries<U> project(Projection<T, U> projection) {
    return new FieldEntryProjectionTransform<T, U>(this, projection);
  }

  public final <U> FieldEntries<T> distinctWhere(final Projection<T, U> onProperty) {
    final Set<U> seenItems = new HashSet<U>();
    return this.filter(new Predicate<T>() {
      @Override
      public boolean test(T item) {
        U projectedValue = onProperty.apply(item);
        return seenItems.add(projectedValue);
      }
    });
  }

  public final <U> Roll<U> roll(Projection<T, U> projection) {
    return new Roll<U>(this.project(projection));
  }

  // Todo: make this abstract and implement.
  public int count() {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<T> iterator() {
    return iterable.iterator();
  }
}
