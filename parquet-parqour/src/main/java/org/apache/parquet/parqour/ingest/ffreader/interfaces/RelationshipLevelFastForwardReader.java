package org.apache.parquet.parqour.ingest.ffreader.interfaces;

/**
 * Created by sircodesalot on 6/23/15.
 */
public interface RelationshipLevelFastForwardReader extends FastForwardReader {
  int nextRelationshipLevel();
}
