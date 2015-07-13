package org.apache.parquet.parqour.ingest.cursor.iface;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableRecordSet;
import org.apache.parquet.parqour.ingest.cursor.iterators.RecordSet;

/**
 * Created by sircodesalot on 6/19/15.
 */
public abstract class AdvanceableCursor implements Cursor {
  private final String name;

  protected int start = 0;
  protected int end = 1;

  public AdvanceableCursor(String name) {
    this.name = name;
    this.start = 0;
    this.end = 1;
  }

  public AdvanceableCursor setRange(int start, int end) {
    this.start = start;
    this.end = end;
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
  public Object value() {
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
  public RollableRecordSet<Integer> i32Iter() {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public RollableRecordSet<Integer> i32Iter(int index) {
    throw new DataIngestException("Invalid path");
  }

  public RollableRecordSet<Integer> i32StartIteration(int startOffset) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public RollableRecordSet<Integer> i32Iter(String path) {
    throw new DataIngestException("Invalid path");
  }

  @Override
  public RecordSet<Cursor> fieldIter(int index) {
    throw new DataIngestException("Invalid path");
  }

  public RecordSet<Cursor> fieldIter() {
    throw new DataIngestException("Invalid path");
  }

  public String name() { return this.name; }
}
