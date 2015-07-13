package org.apache.parquet.parqour.ingest.read.nodes;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.ColumnIngestNodeBase;
import org.apache.parquet.parqour.ingest.read.nodes.impl.i32.Int32NoRepeatIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.i32.Int32RepeatingIngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.PrimitiveType;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 7/13/15.
 */
public class IngestNodeGenerator {
  public static ColumnIngestNodeBase generateIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent, ColumnDescriptor descriptor,
                                                           PrimitiveType schemaNode, DiskInterfaceManager diskInterfaceManager, int childIndex) {
    switch (schemaNode.getRepetition()) {
      case REQUIRED:
      case OPTIONAL:
        return generateNoRepeatIngestNode(schemaInfo, parent, descriptor, schemaNode, diskInterfaceManager, childIndex);
      case REPEATED:
        return generateRepeatingIngestNode(schemaInfo, parent, descriptor, schemaNode, diskInterfaceManager, childIndex);
    }

    throw new NotImplementedException();
  }

  private static ColumnIngestNodeBase generateNoRepeatIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent, ColumnDescriptor descriptor,
                                                        PrimitiveType schemaNode, DiskInterfaceManager diskInterfaceManager, int childIndex) {
    switch (schemaNode.getPrimitiveTypeName()) {
      case INT32:
        return new Int32NoRepeatIngestNode(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, childIndex);
    }

    throw new NotImplementedException();
  }

  private static ColumnIngestNodeBase generateRepeatingIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent, ColumnDescriptor descriptor,
                                                                 PrimitiveType schemaNode, DiskInterfaceManager diskInterfaceManager, int childIndex) {
    switch (schemaNode.getPrimitiveTypeName()) {
      case INT32:
        return new Int32RepeatingIngestNode(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, childIndex);
    }

    throw new NotImplementedException();
  }
}
