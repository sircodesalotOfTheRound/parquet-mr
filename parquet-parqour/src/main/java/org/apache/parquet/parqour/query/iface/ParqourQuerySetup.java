package org.apache.parquet.parqour.query.iface;

import org.apache.parquet.parqour.query.collections.OneToManyMap;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.udf.*;

/**
 * Created by sircodesalot on 7/15/15.
 */
public class ParqourQuerySetup {
  public OneToManyMap<String, ParqourUdf> userDefinedFunctions = new OneToManyMap<String, ParqourUdf>();

  public ParqourQuery query(String query) {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString(query);
    ParqourSource source = new ParqourSource(rootExpression);

    return new ParqourPlainQuery(source, rootExpression);
  }

  public <U> ParqourQuerySetup udf(String name, ZeroParameterUdf<U> function) {
    this.userDefinedFunctions.add(name, function);
    return this;
  }

  public <U> ParqourQuerySetup udf(String name, SingleParameterUdf<U> function) {
    this.userDefinedFunctions.add(name, function);
    return this;
  }

  public <U> ParqourQuerySetup udf(String name, TwoParameterUdf<U> function) {
    this.userDefinedFunctions.add(name, function);
    return this;
  }

  public <U> ParqourQuerySetup udf(String name, ThreeParameterUdf<U> function) {
    this.userDefinedFunctions.add(name, function);
    return this;
  }
}
