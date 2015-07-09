package org.apache.parquet.parqour.ingest.read.nodes.categories;

import org.apache.parquet.parqour.ingest.cursor.GroupAggregateCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.impl.GroupIngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 6/4/15.
 */
public abstract class AggregatingIngestNode extends IngestNode {
  private static final int SCHEMA_DEFINING_CHILD = 0;
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
          ColumnIngestNodeBase.determineReadNodeType(schemaInfo, this,
            schemaInfo.getColumnDescriptorByPath(childPath),
            child,
            diskInterfaceManager,
            childColumnIndex);

        children.add(ingestNodeBase);
      } else {
        children.add(new GroupIngestNode(schemaInfo,
          this,
          childPath,
          (GroupType) child,
          diskInterfaceManager,
          childColumnIndex));
      }
    }

    return new IngestNodeSet(children);
  }

  private GroupAggregateCursor generateAggregateCursor(int childColumnCount) {
    GroupAggregateCursor aggregate = new GroupAggregateCursor(name, childColumnCount, 1000);
    for (IngestNode child : this.children) {
      int[] entries = new int[10000];
      aggregate.setResultSetForChildIndex(child.childColumnIndex(), entries);
      AdvanceableCursor childCursor = child.onLinkToParent(this, entries);
      aggregate.setChildCursor(child.childColumnIndex(), childCursor);
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
      parent.setResultsReported(thisChildColumnIndex, rowNumber);
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
  public final void setSchemaLink(int rowNumber, int repetitionLevel, int definitionLevel, int childLinkIndex, boolean childIsDefined) {
    if (currentRowNumber != rowNumber) {
      relationshipLinkWriteIndex = -1;
      currentRowNumber = rowNumber;
    }

    boolean lastItemWasDefined = definitionLevel >= definitionLevelAtThisNode;

    // Continue upstream if the parent also has a smaller repetitionValue, and this is the schema reporting node.
    if (repetitionLevel <= parentRepetitionLevel) {
      if (lastItemWasDefined) {
        relationshipLinks[++relationshipLinkWriteIndex] = childLinkIndex;
      } else {
        if (relationshipLinkWriteIndex < 0) {
          relationshipLinks[++relationshipLinkWriteIndex] = childLinkIndex;
        }

        int lastRelationshipWriteIndex = relationshipLinkWriteIndex;
        relationshipLinks[++relationshipLinkWriteIndex] = relationshipLinks[lastRelationshipWriteIndex];
      }

      if (isSchemaReportingNode) {
        parent.setSchemaLink(rowNumber, repetitionLevel, definitionLevel, relationshipLinkWriteIndex, lastItemWasDefined);
      }
    }
  }

  public void finishRow(int childLinkIndex) {
    if (hasParent) {
      relationshipLinks[++relationshipLinkWriteIndex] = childLinkIndex;

      if (isSchemaReportingNode) {
        parent.finishRow(++relationshipLinkWriteIndex);
      }
    }
  }

  public String path() { return this.path; }

  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode, int[] relationships) {
    this.relationshipLinks = relationships;
    return aggregate;
  }

}
