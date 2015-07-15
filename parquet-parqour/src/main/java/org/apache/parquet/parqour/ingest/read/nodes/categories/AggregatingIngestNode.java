package org.apache.parquet.parqour.ingest.read.nodes.categories;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.cursor.GroupAggregateCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iterable.field.GroupAggregateIterableCursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeGenerator;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 6/4/15.
 */
public abstract class AggregatingIngestNode extends IngestNode {
  private static final String EMPTY_PATH = "";

  private final String path;
  private final GroupType groupSchema;
  private final IngestNodeSet children;
  private final DiskInterfaceManager diskInterfaceManager;
  private final int childColumnCount;
  private final GroupAggregateCursor aggregate;

  protected Integer[][] schemaLinks;
  protected int relationshipLinkWriteIndex = -1;

  protected AggregatingIngestNode(SchemaInfo schemaInfo, Type schemaNode, DiskInterfaceManager diskInterfaceManager) {
    super(schemaInfo, null, "", schemaNode, IngestNodeCategory.AGGREGATOR, -1);

    this.path = EMPTY_PATH;
    this.groupSchema = schemaInfo.schema();
    this.diskInterfaceManager = diskInterfaceManager;
    this.children = collectChildren(groupSchema, EMPTY_PATH);
    this.childColumnCount = children.size();

    this.ingestBufferLength = 100;
    this.schemaLinks = generateSchemaLinks(childColumnCount, ingestBufferLength );
    this.aggregate = generateAggregateCursor(children, schemaLinks);
  }

  public AggregatingIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent,
                               String fqn, GroupType groupSchema,
                               DiskInterfaceManager diskInterfaceManager, int childNodeIndex) {

    super(schemaInfo, parent, fqn, groupSchema, IngestNodeCategory.AGGREGATOR, childNodeIndex);

    this.path = fqn;
    this.groupSchema = groupSchema;
    this.diskInterfaceManager = diskInterfaceManager;
    this.children = collectChildren(groupSchema, fqn);
    this.childColumnCount = children.size();
    this.ingestBufferLength = 100;
    this.schemaLinks = generateSchemaLinks(childColumnCount, ingestBufferLength );
    this.aggregate = generateAggregateCursor(children, schemaLinks);
  }

  private IngestNodeSet collectChildren(GroupType node, String parentPath) {
    List<IngestNode> children = new ArrayList<IngestNode>();

    for (int childColumnIndex = 0; childColumnIndex < node.getFields().size(); childColumnIndex++) {
      Type child = node.getFields().get(childColumnIndex);
      String childPath = SchemaInfo.computePath(parentPath, child.getName());

      IngestNode newIngestNode;
      if (child.isPrimitive()) {
        ColumnDescriptor columnDescriptor = schemaInfo.getColumnDescriptorByPath(childPath);
        newIngestNode =
          IngestNodeGenerator.generateIngestNode(schemaInfo, this,
            columnDescriptor, child.asPrimitiveType(), diskInterfaceManager, childColumnIndex);

      } else {
        newIngestNode = IngestNodeGenerator.generateAggregationNode(schemaInfo, this,
          childPath, child.asGroupType(), diskInterfaceManager, childColumnIndex);
      }

      children.add(newIngestNode);
    }

    return new IngestNodeSet(children);
  }

  private Integer[][] generateSchemaLinks(int childColumnCount, int ingestBufferLength) {
    return new Integer[childColumnCount][ingestBufferLength];
  }

  private GroupAggregateCursor generateAggregateCursor(IngestNodeSet children, Integer[][] schemaLinks) {
    // The root node is a special case because it's always defined as REPEAT even though we don't treat it as such.
    GroupAggregateCursor aggregate;
    if (this.hasParent && this.repetitionType == Type.Repetition.REPEATED) {
      aggregate = new GroupAggregateIterableCursor(name, schemaLinks);
    }  else {
      aggregate = new GroupAggregateCursor(name, schemaLinks);
    }

    return linkChildren(aggregate, children);
  }

  private GroupAggregateCursor linkChildren(GroupAggregateCursor aggregate, IngestNodeSet children) {
    for (IngestNode child : children) {
      AdvanceableCursor childCursor = child.onLinkToParent(this);
      aggregate.setChildCursor(child.columnIndex(), childCursor);
    }

    return aggregate;
  }


  // Return an immutable iterator.
  public Iterable<IngestNode> children() {
    return children;
  }

  // NOTE: This should only be called by child-0 (the first child node for this aggregator).
  //
  // The RL and DL form a range with an invariant RL <= DL. Every ancestor node with an RL greater than the reported entry value
  // should be linked together into a list. If a node has a higher RL than this entry determines, this means that the
  // entry is not part of the new list. If the entry has a lower DL, than specified by the entry, this means that the item
  // is not defined, and therefore should be connected by using a 'null'  link. Everything in the interval between RL and DL
  // forms the content of the new list.
  public abstract void linkSchema(IngestNode child);
  public abstract void finishRow();

  @Override
  protected void expandIngestBuffer() {
    int newIngestBufferLength = this.ingestBufferLength * 2;

    for (int index = 0; index < childColumnCount; index++) {
      Integer[] newChildColumnIngestBuffer = new Integer[newIngestBufferLength];
      System.arraycopy(schemaLinks[index], 0, newChildColumnIngestBuffer, 0, ingestBufferLength);

      this.schemaLinks[index] = newChildColumnIngestBuffer;
    }

    this.ingestBufferLength = newIngestBufferLength;

    // Todo: move this code into the cursor.
    //this.aggregate.setArray(newIngestBuffer);
  }

  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    return aggregate;
  }

  public String path() { return this.path; }
  public GroupAggregateCursor cursor() { return this.aggregate; }
}
