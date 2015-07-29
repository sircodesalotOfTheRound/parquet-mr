package org.apache.parquet.parqour.queryinfo;

import org.apache.parquet.parqour.ingest.schema.QueryFilePath;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 7/29/15.
 */
public class TestQueryFilePath {
  @Test
  public void testPaths() {
    assertEquals("path.parq", pathFromQueryString("path.parq"));
    assertEquals("relative/path.parq", pathFromQueryString("relative/path.parq"));
    assertEquals("/absolute/path.parq", pathFromQueryString("/absolute/path.parq"));

    assertEquals("path.parq", pathFromQueryString("select * from 'path.parq'"));
    assertEquals("relative/path.parq", pathFromQueryString("select * from 'relative/path.parq'"));
    assertEquals("/absolute/path.parq", pathFromQueryString("select * from '/absolute/path.parq'"));
  }

  public String pathFromQueryString(String query) {
    TextQueryTreeRootExpression expression = TextQueryTreeRootExpression.fromString(query);
    return new QueryFilePath(expression).path();
  }
}
