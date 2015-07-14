package org.apache.parquet.parqour.ingest.read.nodes.categories;

import org.apache.parquet.parqour.ingest.cursor.GroupAggregateCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iterable.aggregation.GroupAggregateIterableCursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeGenerator;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
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

  protected AggregatingIngestNode(SchemaInfo schemaInfo, Type schemaNode, DiskInterfaceManager diskInterfaceManager) {
    super(schemaInfo, null, "", schemaNode, IngestNodeCategory.AGGREGATOR, -1);

    this.path = EMPTY_PATH;
    this.groupSchema = schemaInfo.schema();
    this.diskInterfaceManager = diskInterfaceManager;
    this.children = collectChildren(groupSchema, EMPTY_PATH);
    this.childColumnCount = children.size();

    this.aggregate = generateAggregateCursor(childColumnCount);
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
    this.aggregate = generateAggregateCursor(childColumnCount);
  }

  private IngestNodeSet collectChildren(GroupType node, String parentPath) {
    List<IngestNode> children = new ArrayList<IngestNode>();


    for (int childColumnIndex = 0; childColumnIndex < node.getFields().size(); childColumnIndex++) {
      Type child = node.getFields().get(childColumnIndex);
      String childPath = SchemaInfo.computePath(parentPath, child.getName());

      if (child.isPrimitive()) {
        ColumnIngestNodeBase ingestNodeBase =
          IngestNodeGenerator.generateIngestNode(schemaInfo, this,
            schemaInfo.getColumnDescriptorByPath(childPath),
            (PrimitiveType) child,
            diskInterfaceManager,
            childColumnIndex);

        children.add(ingestNodeBase);
      } else {
        AggregatingIngestNode aggregationNode = IngestNodeGenerator.generateAggregationNode(schemaInfo,
          this, childPath, (GroupType) child, diskInterfaceManager, childColumnIndex);

        children.add(aggregationNode);
      }
    }

    return new IngestNodeSet(children);
  }

  private GroupAggregateCursor generateAggregateCursor(int childColumnCount) {
    GroupAggregateCursor aggregate;
    if (this.hasParent && this.repetitionType == Type.Repetition.REPEATED) {
      // The root node is a special case because it always has a 'repeat' even though we
      // don't treat it as though it does.
      aggregate = new GroupAggregateIterableCursor(name, childColumnCount, 1000);
    }  else {
      aggregate = new GroupAggregateCursor(name, childColumnCount, 1000);
    }

    for (IngestNode child : this.children) {

      // TODO: Write expansion code.
      aggregate.setResultSetForChildIndex(child.columnIndex(), new Integer[1000]);
      AdvanceableCursor childCursor = child.onLinkToParent(this);
      aggregate.setChildCursor(child.columnIndex(), childCursor);
    }

    return aggregate;
  }

  public void setResultsReported(int childColumnIndex, int rowNumber) {
    if (super.currentRowNumber != rowNumber) {
      this.aggregate.clear();
      this.currentRowNumber = rowNumber;
    }

    this.aggregate.setResultsReported(childColumnIndex);
    // If all results are available and we're not at the root, then report
    // the results upstream.
    if (parent != null && this.aggregate.allResultsReported()) {
      parent.setResultsReported(columnIndex, rowNumber);
    }
  }

  public GroupAggregateCursor collectAggregate() {
    return this.aggregate;
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

  /*if (currentRowNumber != rowNumber) {
      relationshipLinkWriteIndex = -1;
      currentRowNumber = rowNumber;
    }

    // If we require a link from the parent:
    if (repetitionLevel <= parentRepetitionLevel) {
      // If the parent is defined:
      if (definitionLevel >= definitionLevelAtThisNode) {
        schemaLinksFromParentToChild[++relationshipLinkWriteIndex] = childLinkIndex;
      } else {
        schemaLinksFromParentToChild[++relationshipLinkWriteIndex] = null;
      }

      // If this node reports schema, then continue upstream:
      if (isSchemaReportingNode) {
        parent.setSchemaLink(rowNumber, repetitionLevel, definitionLevel, relationshipLinkWriteIndex);
      }
    }
  }
  public void finishRow(int childLinkIndex) {
    // If this node reports schema, then continue upstream:
    if (isSchemaReportingNode) {
      parent.finishRow(relationshipLinkWriteIndex);
    }
  }

  public void defineNewSchemaRepetition() {

  }

  public void endSchemaRepetition() {

  }
  */

  public String path() { return this.path; }

  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    return aggregate;
  }

}
