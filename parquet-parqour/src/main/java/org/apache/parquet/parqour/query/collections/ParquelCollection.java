package org.apache.parquet.parqour.query.collections;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sircodesalot on 15/4/3.
 */
public abstract class ParquelCollection<T> implements Iterable<T> {
  public static final ParquelCollection<ParquelExpression> EMPTY = new ParquelCollection<ParquelExpression>() {
    private final List<ParquelExpression> emptyList = new ArrayList<ParquelExpression>();
    @Override
    public Iterable<ParquelExpression> items() {
      return emptyList;
    }
  };

  public abstract Iterable<T> items();

  public boolean any() {
    return this.items().iterator().hasNext();
  }

  public boolean any(Predicate<T> test) {
    for (T item : this.items()) {
      if (test.test(item)) {
        return true;
      }
    }

    return false;
  }

  public boolean all(Predicate<T> test) {
    for (T item : this.items()) {
      if (!test.test(item)) {
        return false;
      }
    }

    return true;
  }

  public <U> ParquelCollection<U> castTo(Class<U> toType) {
    ParquelAppendableCollection<U> castedItems = new ParquelAppendableCollection<U>();
    for (T item : this.items()) {
      castedItems.add((U)item);
    }

    return castedItems;
  }

  public <U> ParquelCollection<U> ofType(Class<U> type) {
    ParquelAppendableCollection<U> itemsOfType = new ParquelAppendableCollection<U>();
    for (T item : this.items()) {
      if (type.isAssignableFrom(item.getClass())) {
        itemsOfType.add((U) item);
      }
    }

    return itemsOfType;
  }

  public <U> ParquelCollection<U> map(Projection<T, U> projection) {
    ParquelAppendableCollection<U> items = new ParquelAppendableCollection<U>();

    for (T item : this.items()) {
      items.add(projection.apply(item));
    }

    return items;
  }

  public ParquelCollection<T> where(Predicate<T> test) {
    ParquelAppendableCollection<T> items = new ParquelAppendableCollection<T>();

    for (T item : this.items()) {
      if (test.test(item)) {
        items.add(item);
      }
    }

    return items;
  }

  public <U> U firstAs(Class<U> type) {
    return (U)this.first();
  }

  public <U> U secondAs(Class<U> type) {
    return (U)this.first();
  }

  public T first() {
    if (this.items() instanceof List) {
      return (T)((List<T>)this.items()).get(0);
    } else {
      Iterator<T> iterator = this.items().iterator();
      return iterator.next();
    }
  }

  public T second() {
    if (this.items() instanceof List) {
      return (T)((List<T>)this.items()).get(1);
    } else {
      Iterator<T> iterator = this.items().iterator();
      iterator.next();
      return iterator.next();
    }
  }

  public T get(int at) {
    if (this.items() instanceof List) {
      return (T)((List<T>)this.items()).get(at);
    } else {
      Iterator<T> iterator = this.items().iterator();
      for (int index = 0; index < (index - 1); index++) {
        iterator.next();
      }

      return iterator.next();
    }
  }

  public int count() {
    if (this.items() instanceof List) {
      return ((List)this.items()).size();

    } else {
      int size = 0;
      Iterator<T> iterator = this.items().iterator();
      while(iterator.hasNext()) {
        size += 1;
      }
      return size;

    }
  }

  @Override
  public Iterator<T> iterator() {
    return this.items().iterator();
  }

}
