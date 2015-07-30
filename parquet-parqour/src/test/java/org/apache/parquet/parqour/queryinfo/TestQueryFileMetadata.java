package org.apache.parquet.parqour.queryinfo;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.schema.QueryFileMetadata;
import org.apache.parquet.parqour.ingest.schema.QueryFilePath;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 7/30/15.
 */
public class TestQueryFileMetadata {
  private static final String DEFAULT_QUERY = "select * from 'test_output.parq'";

  private void writeAndTestSchema(MessageType schema, String query) {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(schema,
        configuration.version(), 1, 1,
        configuration.useDictionary()) {

        @Override
        public void write(ParquetWriter<Group> writer) throws IOException {
          /* NO-OP */
        }
      });

      MessageType textQuerySchema = schemaFromQuery(query);
      assertEquals(schema, textQuerySchema);
    }
  }

  private void testAgainstDefaultQuery(MessageType schema) {
    writeAndTestSchema(schema, DEFAULT_QUERY);
  }

  @Test
  public void testBaseSchema() throws Exception {
    testAgainstDefaultQuery(new MessageType("single_column",
      new PrimitiveType(REQUIRED, INT32, "single_columnSchema")));
  }

  private static MessageType schemaFromQuery(String query) {
    TextQueryTreeRootExpression expression = TextQueryTreeRootExpression.fromString(query);
    QueryFilePath path = new QueryFilePath(expression);
    return new QueryFileMetadata(path).baseSchema();
  }
}
