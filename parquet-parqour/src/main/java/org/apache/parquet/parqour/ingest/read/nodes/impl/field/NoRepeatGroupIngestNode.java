package org.apache.parquet.parqour.ingest.read.nodes.impl.field;

import org.apache.parquet.parqour.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.cursor.implementations.noniterable.field.GroupCursor;
import org.apache.parquet.parqour.ingest.disk.manager.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.schema.GroupType;

import java.util.Arrays;

/**
 * Created by sircodesalot on 6/2/15.
 */
public final class NoRepeatGroupIngestNode extends GroupIngestNode {

  public NoRepeatGroupIngestNode(QueryInfo queryInfo, AggregatingIngestNode aggregatingIngestNode, String childPath, GroupType child,
                                 DiskInterfaceManager diskInterfaceManager, int childColumnIndex) {
    super(queryInfo, aggregatingIngestNode, childPath, child, diskInterfaceManager, childColumnIndex);
  }

  @Override
  protected GroupCursor generateAggregateCursor(IngestNodeSet children, Integer[][] schemaLinks) {
    AdvanceableCursor[] childCursors = linkChildren(children);
    return new GroupCursor(name, columnIndex, childCursors, schemaLinks);
  }

  @Override
  public final void linkSchema(IngestNode child) {
    int childColumnIndex = child.columnIndex();
    int schemaLinkWriteIndex = schemaLinkWriteIndexForColumn[childColumnIndex];
    if (currentRowNumber != child.currentRowNumber()) {
      currentRowNumber = child.currentRowNumber();

      Arrays.fill(schemaLinkWriteIndexForColumn, 0);
      schemaLinkWriteIndex = 0;
    }

    this.currentEntryRepetitionLevel = child.currentEntryRepetitionLevel();
    this.currentEntryDefinitionLevel = child.currentEntryDefinitionLevel();
    this.currentLinkSiteIndex = schemaLinkWriteIndex;

    if (currentEntryDefinitionLevel >= child.nodeDefinitionLevel()) {
      schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = child.currentLinkSiteIndex();
    } else {
      schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = null;
    }

    // If this node reports schema, then continue upstream:
    if (child.isSchemaReportingNode()) {
      parent.linkSchema(this);
    }

    schemaLinkWriteIndexForColumn[childColumnIndex] = schemaLinkWriteIndex;
  }
}
