package org.apache.parquet.parqour.ingest.read.driver;

import org.apache.parquet.parqour.ingest.plan.evaluation.skipchain.SkipChain;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.SkipChainWayPoint;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.WayPoint;
import org.apache.parquet.parqour.ingest.read.nodes.categories.PrimitiveIngestNodeBase;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 7/10/15.
 */
public class ParqourNoPredicateReadDriver extends ParqourReadDriverBase {
  private final PrimitiveIngestNodeBase[] columnIngestNodes;

  public ParqourNoPredicateReadDriver(SchemaInfo schemaInfo) {
    super(schemaInfo);

    this.columnIngestNodes = collectIngestNodes(finalCommitIngestPath);
  }

  private PrimitiveIngestNodeBase[] collectIngestNodes(SkipChain finalCommitIngestPath) {
    List<PrimitiveIngestNodeBase> ingestNodes = new ArrayList<PrimitiveIngestNodeBase>();
    for (WayPoint current = finalCommitIngestPath.path(); current != null; current = current.successPath()) {
      SkipChainWayPoint currentAsSkipChainWayPoint = (SkipChainWayPoint)current;
      PrimitiveIngestNodeBase ingestNodeAsColumnIngestNode = (PrimitiveIngestNodeBase)currentAsSkipChainWayPoint.ingestNode();

      ingestNodes.add(ingestNodeAsColumnIngestNode);
    }

    return ingestNodes.toArray(new PrimitiveIngestNodeBase[ingestNodes.size()]);
  }

  @Override
  public boolean readNext() {
    if (++rowNumber < rowCount) {
      for (PrimitiveIngestNodeBase columnIngestNode : columnIngestNodes) {
        columnIngestNode.read((int)rowNumber);
      }

      return true;
    } else {
      return false;
    }
  }
}
