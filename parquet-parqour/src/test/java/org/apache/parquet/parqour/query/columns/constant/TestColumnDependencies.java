package org.apache.parquet.parqour.query.columns.constant;

import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by sircodesalot on 7/24/15.
 */
public class TestColumnDependencies {
  @Test
  public void testNamedColumnExpression() {
    assertContainsColumns("select *", "*");
    assertContainsColumns("select first, second, third", "first", "second", "third");
    assertContainsColumns("select first.one, second.two, third.three", "first.one", "second.two", "third.three");
    assertContainsColumns("select same, same, same, same, same, different", "same", "different");
    assertContainsColumns("select * where (lhs < rhs)", "*", "lhs", "rhs");
    assertContainsColumns("select one, two where not ((three = four) AND (four = five))",
      "one", "two", "three", "four", "five");
    assertContainsColumns("select one, one, one where (one != two) OR (one = three)",
      "one", "two", "three");
  }


  private void assertContainsColumns(String query, String ... names) {
    Set<String> expectedColumns = new HashSet<String>();
    TransformCollection<String> columnsFromQuery = fromExpression(query).distinct();
    Collections.addAll(expectedColumns, names);

    assertEquals("Number of columns expected did not match number of columns found.",
      expectedColumns.size(), columnsFromQuery.count());

    for (String column : columnsFromQuery) {
      String failMessage = String.format("Set did not contain columns '%s'", column);
      assertTrue(failMessage, expectedColumns.contains(column));
    }
  }

  private TransformCollection<String> fromExpression(String query) {
    TextQueryTreeRootExpression root = TextQueryTreeRootExpression.fromString(query);
    TransformList<String> columns = new TransformList<String>();

    return root.collectColumnDependencies(columns);
  }

}
