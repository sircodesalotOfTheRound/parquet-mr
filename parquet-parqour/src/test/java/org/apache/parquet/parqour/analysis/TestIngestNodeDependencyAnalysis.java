package org.apache.parquet.parqour.analysis;

import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by sircodesalot on 6/22/15.
 */
public class TestIngestNodeDependencyAnalysis {
  public static IngestTree INGEST_TREE = TestTools.generateIngestTreeFromSchema(TestTools.CONTACTS_SCHEMA);

  @Test
  public void testLeafNodeAnalysis() {
    IngestNodeSet singleLeafNodeOwner = INGEST_TREE.collectIngestNodeDependenciesForPaths("owner");
    assertAllAreContained(singleLeafNodeOwner, "owner");

    IngestNodeSet singleLeafNodeOwnerPhoneNumbers = INGEST_TREE.collectIngestNodeDependenciesForPaths("ownerPhoneNumbers");
    assertAllAreContained(singleLeafNodeOwnerPhoneNumbers, "ownerPhoneNumbers");

    IngestNodeSet singleLeafNodeContactName = INGEST_TREE.collectIngestNodeDependenciesForPaths("contacts.name");
    assertAllAreContained(singleLeafNodeContactName, "contacts.name");

    IngestNodeSet singleLeafNodeContactPhoneNumber = INGEST_TREE.collectIngestNodeDependenciesForPaths("contacts.phoneNumber");
    assertAllAreContained(singleLeafNodeContactPhoneNumber, "contacts.phoneNumber");
  }

  @Test
  public void testAggregateNodeAnalysis() {
    IngestNodeSet contactsNodeDependencies = INGEST_TREE.collectIngestNodeDependenciesForPaths("contacts");
    assertAllAreContained(contactsNodeDependencies, "contacts.name", "contacts.phoneNumber");

    IngestNodeSet rootNodeDependencies = INGEST_TREE.collectIngestNodeDependenciesForPaths("");
    assertAllAreContained(rootNodeDependencies, "owner", "ownerPhoneNumbers", "contacts.name", "contacts.phoneNumber");
  }

  public void assertAllAreContained(IngestNodeSet set, String ... paths) {
    assertEquals(set.size(), paths.length);
    for (String path : paths) {
      assertTrue(set.containsPath(path));
    }
  }
}
