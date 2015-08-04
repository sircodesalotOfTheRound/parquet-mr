package org.apache.parquet.parqour.cursor.lookup;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.cursor.iface.AdvanceableCursor;

/**
 * A linear probing map optimized for handling strings. The primary optimizations are
 * are large empty array that stores the key values, as well as testing against same-object
 * string equality (since in practice the same string will be used repeatedly to retrieve
 * a cursor node.
 */
public class CursorHash {
  private static final int EXPANSION_MULTIPLIER = 17; // Ensure large expansions.
  private static final double CAPACITY_THRESHOLD = (3.0 / 4.0);
  private int hashSize = 99; // Extra large hash size to prevent collisions.
  private int totalItems = 0;

  private class Link {
    private Link next;
    private String key;
    private final AdvanceableCursor cursor;

    public Link(String key, AdvanceableCursor cursor, Link next) {
      this.key = key;
      this.cursor = cursor;
      this.next = next;
    }
  }

  private Link[] probeArray = new Link[hashSize];

  public void add(AdvanceableCursor cursor) {
    if (cursor == null) {
      throw new DataIngestException("Cursor must not be null");
    }
    if (!containsKey(cursor.name())) {
      String key = cursor.name();
      int hashCode = Math.abs(key.hashCode()) % hashSize;

      probeArray[hashCode] = new Link(key, cursor, probeArray[hashCode]);

      if (++totalItems >= ((double)probeArray.length * CAPACITY_THRESHOLD)) {
        int newHashSize = hashSize * EXPANSION_MULTIPLIER;
        this.probeArray = expandProbeArray(newHashSize);
        this.hashSize = newHashSize;
      }
    }
  }

  public AdvanceableCursor get(String key) {
    int hashCode = Math.abs(key.hashCode()) % hashSize;
    for (Link current = probeArray[hashCode]; current != null; current = current.next) {
      String currentKey = current.key;
      // Test if either:
      // (1) The strings are the same object.
      // (2) they have the same hash code and they equal.
      if (currentKey == key
        || (currentKey.hashCode() == key.hashCode() && currentKey.equals(key))) {

        // Set the key so that '==' short circuiting will work if the same string is used again.
        current.key = key;
        return current.cursor;
      }
    }

    throw new DataIngestException("Invalid Column Path.");
  }


  public boolean containsKey(String key) {
    int hashCode = Math.abs(key.hashCode()) % hashSize;
    for (Link current = probeArray[hashCode]; current != null; current = current.next) {
      String currentKey = current.key;
      // Test if either:
      // (1) The strings are the same object.
      // (2) they have the same hash code and they equal.
      if (currentKey == key
        || (currentKey.hashCode() == key.hashCode() && currentKey.equals(key))) {

        // Set the key so that '==' short circuiting will work if the same string is used again.
        return true;
      }
    }

    return false;
  }

  public Link[] expandProbeArray(int newHashSize) {
    Link[] oldArray = this.probeArray;
    Link[] newArray = new Link[newHashSize];

    // Copy items and re-align their locations based on hash code.
    for (int index = 0; index < oldArray.length; index++) {
      if (oldArray[index] != null) {
        for (Link current = oldArray[index]; current != null; current = current.next) {
          int hashCode = Math.abs(current.key.hashCode()) % newHashSize;
          newArray[hashCode] = new Link(current.key, current.cursor, newArray[hashCode]);
        }
      }
    }

    return newArray;
  }

}
