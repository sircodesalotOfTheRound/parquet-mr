package org.apache.parquet.parqour.ingest.ffreader.binary;

public class TrieNode {
  private static final int EXPANSION_SIZE = 11; // Large expansion size to quickly accomodate for those nodes regularly used.
  private static final double CAPACITY_THRESHOLD = (10.0 / 11.0);
  private int hashSize = 5; // Small staring size because most nodes will be seldomly used.
  private int totalItems = 0;

  // If we reach this node, then set either the cached string, or cached bytes.
  private String cachedString;
  private byte[] cachedBinary;

  private class Link {
    private Link next;
    private int key;

    private final TrieNode node;

    public Link(int key, TrieNode node, Link next) {
      this.key = key;
      this.node = node;
      this.next = next;
    }
  }

  private Link[] hashArray = new Link[hashSize];

  public TrieNode find(int key) {
    int hashCode = key % hashSize;
    for (Link current = hashArray[hashCode]; current != null; current = current.next) {
      if (current.key == key) {
        return current.node;
      }
    }

    return null;
  }

  public TrieNode add(int key) {
    // Expand the hash if neccesary.
    if (++totalItems >= (hashSize * CAPACITY_THRESHOLD)) {
      int newHashSize = hashSize * EXPANSION_SIZE;
      this.hashArray = expandProbeArray(newHashSize);
      this.hashSize = newHashSize;
    }

    // Set and return the new node.
    int hashCode = key % hashSize;
    Link result = hashArray[hashCode] = new Link(key, new TrieNode(), hashArray[hashCode]);
    return result.node;
  }

  public Link[] expandProbeArray(int newHashSize) {
    Link[] oldArray = this.hashArray;
    Link[] newArray = new Link[newHashSize];

    // Copy items and re-align their locations based on hash code.
    for (int index = 0; index < oldArray.length; index++) {
      if (oldArray[index] != null) {
        for (Link current = oldArray[index]; current != null; current = current.next) {
          int hashCode = current.key % newHashSize;
          newArray[hashCode] = new Link(current.key, current.node, newArray[hashCode]);
        }
      }
    }

    return newArray;
  }

  public String getCachedString() { return this.cachedString; }
  public byte[] getCacehdBinary() { return this.cachedBinary; }

  public void setCachedString(String value) { this.cachedString = value; }
  public void setCachedBinary(byte[] value) { this.cachedBinary = value; }
}
