package org.apache.parquet.parqour.query.iface;

import org.apache.parquet.parqour.query.collections.OneToManyMap;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.udf.ParqourUdf;
import org.apache.parquet.parqour.query.udf.SingleParameterUdf;
import org.apache.parquet.parqour.query.udf.ThreeParameterUdf;
import org.apache.parquet.parqour.query.udf.TwoParameterUdf;

/**
 * Created by sircodesalot on 7/15/15.
 */
public class ParqourQuerySetup {
  public OneToManyMap<Class, ParqourUdf> userDefinedFunctions = new OneToManyMap<Class, ParqourUdf>();

  public ParqourQuery query(String query) {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString(query);
    ParqourSource source = new ParqourSource(rootExpression);

    return new ParqourPlainQuery(source, rootExpression);
  }

  public <U> ParqourQuerySetup udf(SingleParameterUdf<U> function) {
    this.userDefinedFunctions.add(function.getClass(), function);
    return this;
  }

  public <U> ParqourQuerySetup udf(TwoParameterUdf<U> function) {
    this.userDefinedFunctions.add(function.getClass(), function);
    return this;
  }

  public <U> ParqourQuerySetup udf(ThreeParameterUdf<U> function) {
    this.userDefinedFunctions.add(function.getClass(), function);
    return this;
  }
}
