package org.apache.parquet.parqour.ingest.read.nodes;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.PrimitiveIngestNodeBase;
import org.apache.parquet.parqour.ingest.read.nodes.impl.bool.BooleanNoRepeatIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.field.NoRepeatGroupIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.field.RepeatingGroupIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.binary.BinaryNoRepeatIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.i32.Int32NoRepeatIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.i32.Int32RepeatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.i64.Int64NoRepeatIngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 7/13/15.
 */
public class IngestNodeGenerator {
  public static IngestNode generateIngestNode(AggregatingIngestNode parent, Type child, SchemaInfo schemaInfo,
                                              DiskInterfaceManager diskInterfaceManager, int columnIndex) {
    String childPath = SchemaInfo.computePath(parent.path(), child.getName());

    if (child.isPrimitive()) {
      ColumnDescriptor columnDescriptor = schemaInfo.getColumnDescriptorByPath(childPath);
      return IngestNodeGenerator.generatePrimitiveIngestNode(schemaInfo, parent,
          columnDescriptor, child.asPrimitiveType(), diskInterfaceManager, columnIndex);
    } else {
      return IngestNodeGenerator.generateAggregationNode(schemaInfo, parent,
        childPath, child.asGroupType(), diskInterfaceManager, columnIndex);
    }
  }

  private static AggregatingIngestNode generateAggregationNode(SchemaInfo schemaInfo, AggregatingIngestNode parent, String path, GroupType schemaNode,
                                                               DiskInterfaceManager diskInterfaceManager, int columnIndex) {
    switch (schemaNode.getRepetition()) {
      case REQUIRED:
      case OPTIONAL:
        return new NoRepeatGroupIngestNode(schemaInfo, parent, path, schemaNode, diskInterfaceManager, columnIndex);
      case REPEATED:
        return new RepeatingGroupIngestNode(schemaInfo, parent, path, schemaNode, diskInterfaceManager, columnIndex);
    }

    throw new NotImplementedException();
  }

  private static PrimitiveIngestNodeBase generatePrimitiveIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent, ColumnDescriptor descriptor,
                                                                     PrimitiveType schemaNode, DiskInterfaceManager diskInterfaceManager, int columnIndex) {
    switch (schemaNode.getRepetition()) {
      case REQUIRED:
      case OPTIONAL:
        return generateNoRepeatIngestNode(schemaInfo, parent, descriptor, schemaNode, diskInterfaceManager, columnIndex);
      case REPEATED:
        return generateRepeatingIngestNode(schemaInfo, parent, descriptor, schemaNode, diskInterfaceManager, columnIndex);
    }

    throw new NotImplementedException();
  }

  private static PrimitiveIngestNodeBase generateNoRepeatIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent, ColumnDescriptor descriptor,
                                                                    PrimitiveType schemaNode, DiskInterfaceManager diskInterfaceManager, int columnIndex) {
    switch (schemaNode.getPrimitiveTypeName()) {
      case BOOLEAN:
        return new BooleanNoRepeatIngestNode(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, columnIndex);

      case INT32:
        return new Int32NoRepeatIngestNode(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, columnIndex);

      case INT64:
        return new Int64NoRepeatIngestNode(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, columnIndex);

      case BINARY:
        return new BinaryNoRepeatIngestNode(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, columnIndex);
    }

    throw new NotImplementedException();
  }

  private static PrimitiveIngestNodeBase generateRepeatingIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent, ColumnDescriptor descriptor,
                                                                     PrimitiveType schemaNode, DiskInterfaceManager diskInterfaceManager, int columnIndex) {
    switch (schemaNode.getPrimitiveTypeName()) {
      case INT32:
        return new Int32RepeatingIngestNode(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, columnIndex);
    }

    throw new NotImplementedException();
  }
}
