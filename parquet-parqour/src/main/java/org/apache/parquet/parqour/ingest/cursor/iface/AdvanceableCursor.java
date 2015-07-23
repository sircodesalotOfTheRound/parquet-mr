package org.apache.parquet.parqour.ingest.cursor.iface;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.entrysets.FieldEntries;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableFieldEntries;

/**
 * Created by sircodesalot on 6/19/15.
 */
public abstract class AdvanceableCursor implements Cursor {
  private final String name;
  private final int columnIndex;

  protected int index = 0;

  public AdvanceableCursor(String name, int columnIndex) {
    this. name = name;
    this.columnIndex = columnIndex;

    this.index= 0;
  }

  public AdvanceableCursor advanceTo(int index) {
    this.index = index;
    return this;
  }

  @Override
  public Integer i32() {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Integer i32(int index) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Integer i32(String path) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Long i64() {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Long i64(int index) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Long i64(String path) {
    throw new DataIngestException("Invalid path");
  }


  @Override
  public Boolean bool() {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Boolean bool(int index) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Boolean bool(String path) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Object value() {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Object value(int columnIndex) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Object value(String path) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Cursor field(int index) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public Cursor field(String path) {
    throw new DataIngestException("Invalid path");
  }

  @Deprecated
  public RollableFieldEntries<Integer> i32Iter() {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public RollableFieldEntries<Integer> i32Iter(int index) {
    throw new DataIngestException("Invalid path");
  }

  public RollableFieldEntries<Integer> i32StartIteration(int startOffset) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public RollableFieldEntries<Integer> i32Iter(String path) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public FieldEntries<Cursor> fieldIter(String path) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public FieldEntries<Cursor> fieldIter(int index) {
    throw new DataIngestException("Invalid path");
  }

  public FieldEntries<Cursor> fieldIter() {
    throw new DataIngestException("Invalid path");
  }

  public FieldEntries<Cursor> fieldStartIteration(int columIndex, int startOffset) {
    throw new DataIngestException("Invalid path");
  }

  public String name() { return this.name; }
  public int columnIndex() { return this.columnIndex; }
}
