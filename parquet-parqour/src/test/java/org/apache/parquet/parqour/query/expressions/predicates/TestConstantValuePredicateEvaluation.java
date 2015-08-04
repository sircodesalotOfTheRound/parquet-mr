package org.apache.parquet.parqour.query.expressions.predicates;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryWhereExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 8/4/15.
 */
public class TestConstantValuePredicateEvaluation {
  private static final Map<InfixOperator, TestCallback> TESTABLE_BINARY_OPERATORS = generateTestCallbacks();
  private static final List<InfixOperator> OPERATOR_LIST = new ArrayList<InfixOperator>(TESTABLE_BINARY_OPERATORS.keySet());

  static abstract class TestCallback {
    public abstract boolean matches(Object lhs, Object rhs);
  }

  @Test
  public void testAgainstNumericPredicatae() {
    for (int index = 0; index < 10000; index++) {
      int lhs = TestTools.generateRandomPositiveNegativeInt(100);
      InfixOperator operator = TestTools.randomListItem(OPERATOR_LIST);
      int rhs = TestTools.generateRandomPositiveNegativeInt(100);

      String expression = String.format("select * where %s %s %s", lhs, operator, rhs);
      TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString(expression);
      TextQueryWhereExpression where = rootExpression.asSelectStatement().where();
      TextQueryVariableExpression predicate = rootExpression.asSelectStatement().where().predicate();

      boolean actualResult = TESTABLE_BINARY_OPERATORS.get(operator).matches(lhs, rhs);
      boolean predicateResult = predicate.simplify(where).as(TextQueryTestablePredicateExpression.class).test();

      assertEquals(actualResult, predicateResult);
    }
  }

  private static Map<InfixOperator, TestCallback> generateTestCallbacks() {
    Map<InfixOperator, TestCallback> callbacks = new HashMap<InfixOperator, TestCallback>();
    callbacks.put(InfixOperator.EQUALS, new TestCallback() {
      @Override
      public boolean matches(Object lhs, Object rhs) {
        if (lhs != null) {
          return lhs.equals(rhs);
        } else {
          return rhs == null;
        }
      }
    });

    callbacks.put(InfixOperator.NOT_EQUALS, new TestCallback() {
      @Override
      public boolean matches(Object lhs, Object rhs) {
        if (lhs != null) {
          return !lhs.equals(rhs);
        } else {
          return rhs != null;
        }
      }
    });

    callbacks.put(InfixOperator.LESS_THAN, new TestCallback() {
      @Override
      public boolean matches(Object lhs, Object rhs) {
        Comparable lhsComparable = (Comparable) lhs;

        if (lhs != null) {
          return lhsComparable.compareTo(rhs) < 0;
        } else {
          return false;
        }
      }
    });

    callbacks.put(InfixOperator.LESS_THAN_OR_EQUALS, new TestCallback() {
      @Override
      public boolean matches(Object lhs, Object rhs) {
        Comparable lhsComparable = (Comparable) lhs;

        if (lhs != null) {
          return lhsComparable.compareTo(rhs) <= 0;
        } else {
          return false;
        }
      }
    });


    callbacks.put(InfixOperator.GREATER_THAN, new TestCallback() {
      @Override
      public boolean matches(Object lhs, Object rhs) {
        Comparable lhsComparable = (Comparable) lhs;

        if (lhs != null) {
          return lhsComparable.compareTo(rhs) > 0;
        } else {
          return false;
        }
      }
    });

    callbacks.put(InfixOperator.GREATER_THAN_OR_EQUALS, new TestCallback() {
      @Override
      public boolean matches(Object lhs, Object rhs) {
        Comparable lhsComparable = (Comparable) lhs;

        if (lhs != null) {
          return lhsComparable.compareTo(rhs) >= 0;
        } else {
          return false;
        }
      }
    });

    return callbacks;
  }
}
