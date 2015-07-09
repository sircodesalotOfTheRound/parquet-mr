package org.apache.parquet.parqour.ffreaders.trie;

import org.apache.parquet.parqour.ingest.ffreader.binary.TrieNode;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/25/15.
 */
public class TestBinaryTrieNode {

  @Test
  public void testSingleWordAdd() {
    TrieNode root = new TrieNode();
    String quickbrownFox = "quick brown fox";

    boolean wasNewlyAdded = addStringToTrie(root, quickbrownFox);

    assertTrue(wasNewlyAdded);
    assertEquals(quickbrownFox, findStringInTrie(root, quickbrownFox));
  }

  @Test
  public void testAddingSameWordTwice() {
    TrieNode root = new TrieNode();
    String camptownLady = "the camptown lady";

    boolean wasNewlyAdded = addStringToTrie(root, camptownLady);
    assertTrue(wasNewlyAdded);
    boolean wasNewlyAddedAgain = addStringToTrie(root, camptownLady);
    assertFalse(wasNewlyAddedAgain);


    assertEquals(camptownLady, findStringInTrie(root, camptownLady));
  }

  public boolean addStringToTrie(TrieNode root, String value) {
    // Traverse to the letter.
    TrieNode current = root;
    for (byte letter : value.getBytes()) {
      TrieNode next;
      if ((next = current.find(letter)) != null) {
        current = next;
      } else {
        current = current.add(letter);
      }
    }

    // Cache the string.
    if (current.getCachedString() == null) {
      current.setCachedString(value);
      return true;
    } else {
      return false;
    }
  }

  @Test
  public void addManyRandomStringsToTrie() {
    TrieNode root = new TrieNode();
    List<String> strings = new ArrayList<String>();
    int range = ('z' - 'A');

    for (int index = 0; index < TestTools.generateRandomInt(10000); index++) {
      StringBuilder randomString = new StringBuilder();
      for (int stringIndex = 0; stringIndex < TestTools.generateRandomInt(100); stringIndex++) {
        char letter = (char)('A' + TestTools.generateRandomInt(range));
        randomString.append(letter);
        randomString.append(TestTools.generateRandomInt(10));
      }

      strings.add(randomString.toString());
    }

    // Add the strings to the trie.
    for (String string : strings) {
      addStringToTrie(root, string);
    }

    // Make sure they're all there.
    for (String string : strings) {
      String resultFromTrie = findStringInTrie(root, string);
      assertEquals(string, resultFromTrie);
    }
  }

  public String findStringInTrie(TrieNode root, String value) {
    TrieNode current = root;
    for (byte letter : value.getBytes()) {
      current = current.find(letter);
      if (current == null) {
        return null;
      }
    }

    return current.getCachedString();
  }
}
