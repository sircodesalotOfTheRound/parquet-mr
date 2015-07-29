package org.apache.parquet.parqour.ingest.driver;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.driver.ParqourPredicateReadDriver;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.parqour.testtools.TestTools.EMPTY_CONFIGURATION;
import static org.apache.parquet.parqour.testtools.TestTools.TEST_FILE_PATH;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/22/15.
 */
public class TestReadDriverWithFastForwardEqualsPredicate {
  private static int TOTAL_ROWS = 1000000;
  private static int ROW_TO_SEARCH_FOR = TestTools.generateRandomInt(TOTAL_ROWS);
  private static final Operators.IntColumn PREDICATE_COLUMN = FilterApi.intColumn("i32");
  private static final Operators.Eq<Integer> EQUALS_PREDICATE = FilterApi.eq(PREDICATE_COLUMN, ROW_TO_SEARCH_FOR);

  // If a PrimitiveType node and all of it's ancestors (except for the root, which is always REPEATED), have
  // a RepeatType of 'REQUIRED' then we can use fast forwarding. This setup will fall back to slow-forwarding.
  private final MessageType COUNTING_SCHEMA = new MessageType("multipliers",
    new PrimitiveType(REQUIRED, INT32, "i32"),
    new PrimitiveType(REQUIRED, INT64, "i64"),
    new PrimitiveType(REQUIRED, BOOLEAN, "bool"));

  public void generateTestData(ParquetConfiguration configuration) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, configuration.version(), 1, 10, configuration.useDictionary()) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group countingGroup = new SimpleGroup(COUNTING_SCHEMA)
            .append("i32", index)
            .append("i64", (long)(index * 2))
            .append("bool", (index % 3 == 0));

          writer.write(countingGroup);
        }
      }
    });
  }

  @Test
  public void testReadDriverWithEqualsPredicate() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      this.generateTestData(configuration);
      TestTools.repeat(1, new TestTools.RepeatCallback() {
        @Override
        public void execute() throws Exception {
          ParquetMetadata metadata = ParquetFileReader.readFooter(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);
          QueryInfo queryInfo = new QueryInfo(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), metadata, COUNTING_SCHEMA, EQUALS_PREDICATE);
          ParqourPredicateReadDriver driver = new ParqourPredicateReadDriver(queryInfo);

          while (driver.readNext()) {
            // Should have all results reported, and there should be just one item.
            Cursor cursor = driver.cursor();

            assertEquals((Integer) ROW_TO_SEARCH_FOR, cursor.i32("i32"));
            assertEquals((Long)(long)(ROW_TO_SEARCH_FOR * 2), cursor.i64("i64"));
            assertEquals((ROW_TO_SEARCH_FOR % 3 == 0), cursor.bool("bool"));
          }
        }
      });
    }
  }
}
