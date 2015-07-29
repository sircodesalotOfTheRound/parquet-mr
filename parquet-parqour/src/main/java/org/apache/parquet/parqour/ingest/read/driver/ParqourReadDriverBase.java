package org.apache.parquet.parqour.ingest.read.driver;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.plan.analysis.PredicateAnalysis;
import org.apache.parquet.parqour.ingest.plan.evaluation.EvaluationPathAnalysis;
import org.apache.parquet.parqour.ingest.plan.evaluation.skipchain.SkipChain;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;

/**
 * Created by sircodesalot on 7/10/15.
 */
public abstract class ParqourReadDriverBase {
  protected final QueryInfo queryInfo;
  protected final DiskInterfaceManager diskInterfaceManager;
  protected final IngestTree ingestTree;
  protected final PredicateAnalysis predicateAnalysis;
  protected final EvaluationPathAnalysis pathAnalysis;
  protected final SkipChain finalCommitIngestPath;

  protected final long rowCount;
  protected long rowNumber;

  public ParqourReadDriverBase(QueryInfo queryInfo) {
    this.queryInfo = queryInfo;
    this.diskInterfaceManager = new DiskInterfaceManager(queryInfo);
    this.ingestTree = new IngestTree(queryInfo, diskInterfaceManager);

    this.predicateAnalysis = new PredicateAnalysis(ingestTree);
    this.pathAnalysis = new EvaluationPathAnalysis(ingestTree, predicateAnalysis);

    this.finalCommitIngestPath = pathAnalysis.finalCommitIngestPath();

    this.rowCount = queryInfo.totalRowCount();
    this.rowNumber = -1;
  }

  public abstract boolean readNext();

  public Cursor cursor() {
    return ingestTree.root().cursor();
  }

  public static ParqourReadDriverBase determineReadDriverFromSchemaInfo(QueryInfo queryInfo) {
    if (queryInfo.hasPredicate()) {
      return new ParqourPredicateReadDriver(queryInfo);
    } else {
      return new ParqourNoPredicateReadDriver(queryInfo);
    }
  }
}
