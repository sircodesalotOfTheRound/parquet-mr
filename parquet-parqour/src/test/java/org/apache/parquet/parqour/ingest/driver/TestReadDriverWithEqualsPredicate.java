package org.apache.parquet.parqour.ingest.driver;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.driver.ParqourReadDriver;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.apache.parquet.parqour.testtools.TestTools.EMPTY_CONFIGURATION;
import static org.apache.parquet.parqour.testtools.TestTools.TEST_FILE_PATH;
import static org.junit.Assert.assertEquals;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/22/15.
 */
public class TestReadDriverWithEqualsPredicate {
  private static int TOTAL_ROWS = 1000000;
  private static int ROW_TO_SEARCH_FOR = TestTools.generateRandomInt(TOTAL_ROWS);
  private static final Operators.IntColumn COLUMN = FilterApi.intColumn("one");
  private static final Operators.Eq<Integer> EQUALS_PREDICATE = FilterApi.eq(COLUMN, ROW_TO_SEARCH_FOR);

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multipliers",
    new PrimitiveType(REQUIRED, INT32, "one"),
    new PrimitiveType(REQUIRED, INT32, "two"),
    new PrimitiveType(REQUIRED, INT32, "three"));

  public void generateTestData(ParquetConfiguration configuration) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, configuration.version(), 1, 10, configuration.useDictionary()) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group countingGroup = new SimpleGroup(COUNTING_SCHEMA)
            .append("one", index)
            .append("two", index * 2)
            .append("three", index * 3);

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
          SchemaInfo schemaInfo = new SchemaInfo(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), metadata, COUNTING_SCHEMA, EQUALS_PREDICATE);
          ParqourReadDriver driver = new ParqourReadDriver(schemaInfo);

          while (driver.readNext()) {
            // Should have all results reported, and there should be just one item.
            Cursor cursor = driver.cursor();

            // TODO: Broken test.
            assertEquals((Integer) ROW_TO_SEARCH_FOR, cursor.i32("one"));
            assertEquals((Integer) (ROW_TO_SEARCH_FOR * 2), cursor.i32("two"));
            assertEquals((Integer) (ROW_TO_SEARCH_FOR * 3), cursor.i32("three"));
          }
        }
      });
    }
  }
}
