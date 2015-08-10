package org.apache.parquet.parqour.tools;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sircodesalot on 15/4/3.
 */
public abstract class TransformCollection<T> implements Iterable<T> {
  public static final TransformCollection<TextQueryExpression> EMPTY = new TransformCollection<TextQueryExpression>() {
    private final List<TextQueryExpression> emptyList = new ArrayList<TextQueryExpression>();
    @Override
    public Iterable<TextQueryExpression> items() {
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

  public <U> TransformCollection<U> castTo(Class<U> toType) {
    TransformList<U> castedItems = new TransformList<U>();
    for (T item : this.items()) {
      castedItems.add((U)item);
    }

    return castedItems;
  }

  public <U> TransformCollection<U> ofType(Class<U> type) {
    TransformList<U> itemsOfType = new TransformList<U>();
    for (T item : this.items()) {
      if (type.isAssignableFrom(item.getClass())) {
        itemsOfType.add((U) item);
      }
    }

    return itemsOfType;
  }

  public <U> TransformCollection<U> map(Projection<T, U> projection) {
    TransformList<U> items = new TransformList<U>();

    for (T item : this.items()) {
      items.add(projection.apply(item));
    }

    return items;
  }

  public TransformCollection<T> where(Predicate<T> test) {
    TransformList<T> items = new TransformList<T>();

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
    return (U)this.second();
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

  public <U> TransformCollection<U> flatten(Projection<T, Iterable<U>> onProperty) {
    TransformList<U> flattenedList = new TransformList<U>();
    for (T group : this) {
      for (U subItem : onProperty.apply(group)) {
        flattenedList.add(subItem);
      }
    }

    return flattenedList;
  }

  @Override
  public Iterator<T> iterator() {
    return this.items().iterator();
  }

}
