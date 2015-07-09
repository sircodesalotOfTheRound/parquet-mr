package org.apache.parquet.parqour.ingest.tree.nodes;

import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by sircodesalot on 6/22/15.
 */
public class TestIngestNodeSet {
  private static final IngestNode FIRST = mockNode("first");
  private static final IngestNode SECOND = mockNode("second");
  private static final IngestNode THIRD = mockNode("third");
  private static final IngestNode FOURTH = mockNode("fourth");
  private static final IngestNode FIFTH = mockNode("fifth");

  @Test
  public void testIngestNodeAddition() {
    IngestNodeSet oneTwoThree = new IngestNodeSet(FIRST, SECOND, THIRD);
    assertAllAreContained(oneTwoThree, FIRST, SECOND, THIRD);

    IngestNodeSet fourFive = new IngestNodeSet(FOURTH, FIFTH);
    assertAllAreContained(fourFive, FOURTH, FIFTH);

    IngestNodeSet total = new IngestNodeSet();
    total.addAll(oneTwoThree);
    total.addAll(fourFive);

    assertAllAreContained(total, FIRST, SECOND, THIRD, FOURTH, FIFTH);

    oneTwoThree.addAll(oneTwoThree);
    assertAllAreContained(oneTwoThree, FIRST, SECOND, THIRD);

    IngestNodeSet none = new IngestNodeSet();
    assertAllAreContained(none);

    total.addAll(none);
    total.addAll(oneTwoThree);
    total.addAll(total);

    assertAllAreContained(total, FIRST, SECOND, THIRD, FOURTH, FIFTH);
  }

  @Test
  public void testIngestNodeRemoval() {
    IngestNodeSet oneTwoThree = new IngestNodeSet(FIRST, SECOND, THIRD);
    IngestNodeSet threeFourFive = new IngestNodeSet(THIRD, FOURTH, FIFTH);

    oneTwoThree.removeAll(threeFourFive);
    assertAllAreContained(oneTwoThree, FIRST, SECOND);

    oneTwoThree.removeAll(FIRST);
    assertAllAreContained(oneTwoThree, SECOND);

    oneTwoThree.removeAll(FIFTH);
    assertAllAreContained(oneTwoThree, SECOND);

    oneTwoThree.removeAll(SECOND);
    assertAllAreContained(oneTwoThree);
  }

  public void assertAllAreContained(IngestNodeSet set, IngestNode ... nodes) {
    assertEquals(set.size(), nodes.length);
    for (IngestNode node : nodes) {
      assertTrue(set.containsPath(node.path()));
    }
  }

  private static IngestNode mockNode(String path) {
    IngestNode mock = mock(IngestNode.class);
    when(mock.path()).thenReturn(path);
    return mock;
  }

}
