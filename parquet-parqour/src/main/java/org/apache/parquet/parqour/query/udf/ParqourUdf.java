package org.apache.parquet.parqour.query.udf;

/**
 * Created by sircodesalot on 7/15/15.
 */
public abstract class ParqourUdf {
  private UdfParameterLength length;

  public ParqourUdf(UdfParameterLength length) {
    this.length = length;
  }

  public UdfParameterLength length() { return this.length; }
}
