package org.apache.parquet.parqour.ingest.read.nodes.categories;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.Type;

/**
 * Created by sircodesalot on 6/2/15.
 */
public abstract class IngestNode {
  protected final SchemaInfo schemaInfo;

  protected final AggregatingIngestNode parent;
  protected final int repetitionLevelAtThisNode;
  protected final int definitionLevelAtThisNode;
  protected final Type.Repetition repetitionType;
  protected final boolean isSchemaReportingNode;

  protected final Type schemaNode;
  protected final String path;
  protected final String name;
  protected final IngestNodeCategory category;
  protected final int columnIndex;
  protected final boolean canPerformTrueFastForwards;

  protected final long totalRowCount;

  protected final int parentRepetitionLevel;
  protected final int parentDefinitionLevel;
  protected final boolean hasParent;
  protected final boolean parentIsRepeating;

  protected long currentRowNumber = 0;


  protected int currentEntryDefinitionLevel;
  protected int currentEntryRepetitionLevel;
  protected int currentLinkSiteIndex;

  protected int ingestBufferLength;

  public IngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent, String path, Type schemaNode, IngestNodeCategory category, int childNodeIndex) {
    this.schemaInfo = schemaInfo;
    this.parent = parent;
    this.path = path;
    this.name = schemaNode.getName();
    this.category = category;
    this.schemaNode = schemaNode;
    this.columnIndex = childNodeIndex;
    this.hasParent = determineHasParent();
    this.totalRowCount = schemaInfo.totalRowCount();
    this.repetitionType = schemaNode.getRepetition();
    this.repetitionLevelAtThisNode = schemaInfo.determineRepeatLevel(this);
    this.definitionLevelAtThisNode = schemaInfo.getDefinitionLevel(path);
    this.canPerformTrueFastForwards = determineCanPerformTrueFastForwarding(this);
    this.isSchemaReportingNode = determineIsSchemaReportingNode(childNodeIndex);
    this.parentRepetitionLevel = computeParentRepetitionLevel();
    this.parentDefinitionLevel = computeParentDefinitionLevel();
    this.parentIsRepeating = determineIfParentIsRepeating();
  }

  // By definition, the schema beyond the first common parent should be equal. For example, if there are two columns, 'A' and 'B',
  // but 'A' reports a different number of 'zero' repetition levels from 'B', then 'A' and 'B' have a different number of rows, which
  // implies that the data is invalid - all columns should have the same number of rows. Proceeding by induction, every node beyond the first
  // common ancestor level between two columns must report the same number of parents - if 'A' and 'B' share a sublist ancestor, then the number
  // of sublists shared between 'A' and 'B' must be equal. Since this is the case, the schema only needs to be reported upstream by the first child node
  // of any parent (since all other children will report the exact same schema), hence we only permit 'child-0' to send schema information upstream.
  private boolean determineIsSchemaReportingNode(int childNodeIndex) {
    return childNodeIndex == 0;
  }
  // A node can perform true fast forwards (fast-forwards through entrie pages) if it, and all of it's parents are listed as 'REQUIRED'.
  // This means that the rows are one-to-one with the entries, and we can compute fast-forwards based on
  // entry number. Otherwise, we can skip entries, but we cannot perform true fast-forwards.
  private boolean determineCanPerformTrueFastForwarding(IngestNode node) {
    if (node == null) return true;
    if (node.schemaNode().getRepetition() == Type.Repetition.REQUIRED) {
      return determineCanPerformTrueFastForwarding(node.parent);
    } else {
      // If we've reached the root level without hitting an OPTIONAL, or REPEAT, then this
      // node can perform fast forwards (since the root is always REPEAT).
      if (!node.hasParent) {
        return true;
      } else {
        return false;
      }
    }
  }

  // The RL and DL relationship values determine which items to link together into a new (horizontal) linked-list.
  // The linked list starts with the topmost node having the RL value, and ends with the bottom-most node having
  // value equal to the DL value. Since this range begins wtih the RL value, if this node has no parent, then we set it to
  // an impossibly low value, meaning it will never be captured into a list. This saves us having to perform parent-null tests.
  private int computeParentRepetitionLevel() {
    return this.hasParent() ? parent.repetitionLevelAtThisNode : Integer.MIN_VALUE;
  }

  private int computeParentDefinitionLevel() {
    return this.hasParent() ? parent.definitionLevelAtThisNode : Integer.MAX_VALUE;
  }

  private boolean determineHasParent() {
    return this.parent != null;
  }

  public boolean determineIfParentIsRepeating() {
    // The root node is a special case. It is always listed as 'REPEAT' even though
    // we don't treat it as though it does.
    if (this.hasParent && this.parent.parent != null) {
      return this.parent.repetitionType == Type.Repetition.REPEATED;
    }

    return false;
  }

  protected abstract AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode);
  protected abstract void expandIngestBuffer();

  public String name() { return this.name; }
  public int columnIndex() { return this.columnIndex; }
  public Type schemaNode() { return this.schemaNode; }
  public AggregatingIngestNode parent() { return this.parent; }
  public boolean hasParent() { return this.hasParent; }
  public String path() { return this.path; }

  public boolean canPerformTrueFastForwards() { return this.canPerformTrueFastForwards; }
  public IngestNodeCategory category() { return this.category; }
  public Type.Repetition repetitionType() { return schemaNode.getRepetition(); }
  public int nodeRepetitionLevel() { return this.repetitionLevelAtThisNode; }
  public int nodeDefinitionLevel() { return this.definitionLevelAtThisNode; }

  public final int currentEntryDefinitionLevel() { return this.currentEntryDefinitionLevel; }
  public final int currentEntryRepetitionLevel() { return this.currentEntryRepetitionLevel; }
  public final boolean isSchemaReportingNode() { return this.isSchemaReportingNode; }
  public final int currentLinkSiteIndex() { return this.currentLinkSiteIndex; }
  public final long currentRowNumber() { return this.currentRowNumber; }
}
