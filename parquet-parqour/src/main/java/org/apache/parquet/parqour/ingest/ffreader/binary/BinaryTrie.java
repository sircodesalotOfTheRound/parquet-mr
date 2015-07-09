package org.apache.parquet.parqour.ingest.ffreader.binary;

import java.util.Arrays;

/**
 * Created by sircodesalot on 6/25/15.
 */
public class BinaryTrie {
  private final TrieNode root;
  private final int maxLength;
  private final int maxSize;
  private int size;

  public BinaryTrie(int maxSize, int maxLength) {
    this.root = new TrieNode();
    this.maxSize = maxSize;
    this.maxLength = maxLength;
    this.size = 0;
  }

  public String getString(byte[] data, int offset, int length) {
    // (1) If we're still under max-length/max-size then go ahead and add the string.
    // (2) Otherwise see if the item is in the trie.
    // (3) If it isn't in the trie, then just generate the string normally.
    if (length < maxLength && size < maxSize) {
      // Traverse to where the node would be (traverse creates a path if there isn't already one).
      TrieNode node = traverse(data, offset, length);

      // Use the string if it's there, otherwise cache it.
      String result;
      if ((result = node.getCachedString()) != null) {
        return result;
      } else {
        result = new String(data, offset, length);
        node.setCachedString(result);
        return result;

      }

    } else if (length < maxLength) {
      TrieNode found = find(data, offset, length);
      String result;
      if (found != null && (result = found.getCachedString()) != null) {
        return result;
      }
    }

    // All else fails, just generate the string.
    return new String(data, offset, length);
  }

  public byte[] getByteArray(byte[] data, int offset, int length) {
    // (1) If we're still under max-length/max-size then go ahead and add the string.
    // (2) Otherwise see if the item is in the trie.
    // (3) If it isn't in the trie, then just generate the string normally.
    if (length < maxLength && size < maxSize) {
      // Traverse to where the node would be (traverse creates a path if there isn't already one).
      TrieNode node = traverse(data, offset, length);

      // Use the string if it's there, otherwise cache it.
      byte[] result;
      if ((result = node.getCacehdBinary()) != null) {
        return result;
      } else {
        result = Arrays.copyOfRange(data, offset, offset + length);
        node.setCachedBinary(result);
        return result;

      }

    } else if (length < maxLength) {
      TrieNode found = find(data, offset, length);
      byte[] result;
      if (found != null && (result = found.getCacehdBinary()) != null) {
        return result;
      }
    }

    // All else fails, just generate the array
    return Arrays.copyOfRange(data, offset, offset + length);
  }

  private TrieNode traverse(byte[] data, int offset, int length) {
    TrieNode current = root;
    int upTo = offset + length;

    // Traverse to the letter.
    for (int index = offset; index < upTo; index++) {
      byte abyte = data[index];
      TrieNode next;
      if ((next = current.find(abyte)) != null) {
        current = next;
      } else {
        current = current.add(abyte);
      }
    }

    return current;
  }

  private TrieNode find(byte[] data, int offset, int length) {
    TrieNode current = root;
    int upTo = offset + length;

    // Traverse to the letter.
    for (int index = offset; index < upTo; index++) {
      byte abyte = data[index];
      TrieNode next;
      if ((next = current.find(abyte)) != null) {
        current = next;
      } else {
        return null;
      }
    }

    return current;
  }

  public int size() {
    return this.size;
  }
}
