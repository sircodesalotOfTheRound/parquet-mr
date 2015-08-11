package org.apache.parquet.parqour.ingest.read.nodes.categories;

import org.apache.parquet.parqour.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager_OLD;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeGenerator;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 6/4/15.
 */
public abstract class AggregatingIngestNode extends IngestNode {
  // TODO: Create Indexes class to limit the amount of 'Integers' we instantiate.
  private static final String EMPTY_PATH = "";

  protected final String path;
  protected final GroupType groupSchema;
  protected final IngestNodeSet children;
  protected final DiskInterfaceManager_OLD diskInterfaceManager;
  protected final int childColumnCount;

  protected int schemaLinkWriteIndexForColumn[];

  protected AggregatingIngestNode(QueryInfo queryInfo, Type schemaNode, DiskInterfaceManager_OLD diskInterfaceManager) {
    super(queryInfo, null, "", schemaNode, IngestNodeCategory.AGGREGATOR, -1);

    this.path = EMPTY_PATH;
    this.groupSchema = queryInfo.schema();
    this.diskInterfaceManager = diskInterfaceManager;
    this.children = collectChildren(groupSchema);
    this.childColumnCount = children.size();

    this.schemaLinkWriteIndexForColumn = new int[childColumnCount];
    this.ingestBufferLength = 100;
  }

  public AggregatingIngestNode(QueryInfo queryInfo, AggregatingIngestNode parent,
                               String fqn, GroupType groupSchema,
                               DiskInterfaceManager_OLD diskInterfaceManager, int childNodeIndex) {

    super(queryInfo, parent, fqn, groupSchema, IngestNodeCategory.AGGREGATOR, childNodeIndex);

    this.path = fqn;
    this.groupSchema = groupSchema;
    this.diskInterfaceManager = diskInterfaceManager;
    this.children = collectChildren(groupSchema);
    this.childColumnCount = children.size();
    this.ingestBufferLength = 100;
    this.schemaLinkWriteIndexForColumn = new int[childColumnCount];
  }

  private IngestNodeSet collectChildren(GroupType node) {
    List<IngestNode> children = new ArrayList<IngestNode>();

    for (int columnIndex = 0; columnIndex < node.getFields().size(); columnIndex++) {
      Type child = node.getFields().get(columnIndex);
      IngestNode ingestNode = IngestNodeGenerator
        .generateIngestNode(this, child, queryInfo, diskInterfaceManager, columnIndex);

      children.add(ingestNode);
    }

    return new IngestNodeSet(children);
  }

  protected AdvanceableCursor[] linkChildren(IngestNodeSet children) {
    AdvanceableCursor[] childCursors = new AdvanceableCursor[children.size()];

    for (IngestNode child : children) {
      childCursors[child.columnIndex()] = child.onLinkToParent(this);
    }

    return childCursors;
  }


  // Return an immutable iterator.
  // Todo: Remove this.
  @Deprecated
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

  public String path() { return this.path; }
}
