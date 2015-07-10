package org.apache.parquet.parqour.ingest.read.driver;

import org.apache.parquet.parqour.ingest.plan.evaluation.skipchain.SkipChain;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.SkipChainWayPoint;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.WayPoint;
import org.apache.parquet.parqour.ingest.read.nodes.categories.ColumnIngestNodeBase;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 7/10/15.
 */
public class ParqourNoPredicateReadDriver extends ParqourReadDriver {
  private final ColumnIngestNodeBase[] columnIngestNodes;

  public ParqourNoPredicateReadDriver(SchemaInfo schemaInfo) {
    super(schemaInfo);

    this.columnIngestNodes = collectIngestNodes(finalCommitIngestPath);
  }

  private ColumnIngestNodeBase[] collectIngestNodes(SkipChain finalCommitIngestPath) {
    List<ColumnIngestNodeBase> ingestNodes = new ArrayList<ColumnIngestNodeBase>();
    for (WayPoint current = finalCommitIngestPath.path(); current != null; current = current.successPath()) {
      SkipChainWayPoint currentAsSkipChainWayPoint = (SkipChainWayPoint)current;
      ColumnIngestNodeBase ingestNodeAsColumnIngestNode = (ColumnIngestNodeBase)currentAsSkipChainWayPoint.ingestNode();

      ingestNodes.add(ingestNodeAsColumnIngestNode);
    }

    return ingestNodes.toArray(new ColumnIngestNodeBase[ingestNodes.size()]);
  }

  @Override
  public boolean readNext() {
    if (++rowNumber < rowCount) {
      for (ColumnIngestNodeBase columnIngestNode : columnIngestNodes) {
        columnIngestNode.read((int)rowNumber);
      }

      return true;
    } else {
      return false;
    }
  }
}
