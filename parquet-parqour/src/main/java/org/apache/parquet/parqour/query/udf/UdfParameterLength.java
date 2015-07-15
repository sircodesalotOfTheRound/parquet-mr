package org.apache.parquet.parqour.query.udf;

/**
 * Created by sircodesalot on 7/15/15.
 */
public enum UdfParameterLength {
  ZERO(0),
  ONE(1),
  TWO(2),
  THREE(3);

  private final int length;

  UdfParameterLength(int length) {
    this.length = length;
  }

  public int length() { return this.length; }
}
