package org.apache.parquet.parqour.ingest.read.nodes.impl.binary;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.implementations.noniterable.i32.Int32Cursor;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.PrimitiveIngestNodeBase;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.schema.Type;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 6/11/15.
 */
public final class BinaryNoRepeatIngestNode extends PrimitiveIngestNodeBase<Int32FastForwardReader> {
  private int currentValue = 0;

  // TODO: Write expansion code.
  private Integer[] rowVector = new Integer[100000];

  private final Int32Cursor cursor = new Int32Cursor(name, columnIndex, rowVector);

  public BinaryNoRepeatIngestNode(QueryInfo queryInfo,
                                  AggregatingIngestNode parent,
                                  Type schemaNode,
                                  ColumnDescriptor descriptor,
                                  DiskInterfaceManager diskInterfaceManager,
                                  int childIndex) {

    super(queryInfo, parent, schemaNode, descriptor, diskInterfaceManager, childIndex);

  }

  @Override
  protected void updateValuesReaderValue() {
    this.currentValue = valuesReader.readi32();
  }

  @Override
  protected void expandIngestBuffer() {

  }

  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    return cursor;
  }

  // Heavily inlined for performance.
  @Override
  public void read(int rowNumber) {
    throw new NotImplementedException();
  }

}
