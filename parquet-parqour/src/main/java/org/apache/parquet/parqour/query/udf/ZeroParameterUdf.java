package org.apache.parquet.parqour.query.udf;

/**
 * Created by sircodesalot on 7/15/15.
 */
public abstract class ZeroParameterUdf<T> extends ParqourUdf {
  public ZeroParameterUdf() {
    super(UdfParameterLength.ZERO);
  }

  public abstract T apply();
}
