package org.apache.parquet.parqour.ingest.driver;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.driver.ParqourNoPredicateReadDriver;
import org.apache.parquet.parqour.ingest.read.driver.ParqourReadDriverBase;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/22/15.
 */
public class TestNoPredicateReadDriver {
  private static int TOTAL_ROWS = 1000000;
  private static int ROW_TO_SEARCH_FOR = TestTools.generateRandomInt(TOTAL_ROWS);

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
  public void testReadDriver() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      this.generateTestData(configuration);

      TestTools.repeat(1, new TestTools.RepeatCallback() {
        @Override
        public void execute() throws Exception {
          ParquetMetadata metadata = ParquetFileReader.readFooter(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);
          SchemaInfo schemaInfo = new SchemaInfo(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), metadata, COUNTING_SCHEMA);
          ParqourReadDriverBase driver = new ParqourNoPredicateReadDriver(schemaInfo);

          for (int index = 0; index < TOTAL_ROWS; index++) {
            driver.readNext();
            // Should have all results reported, and there should be just one item.
            Cursor cursor = driver.cursor();

            assertEquals((Integer) index, cursor.i32("one"));
            assertEquals((Integer) (index * 2), cursor.i32("two"));
            assertEquals((Integer) (index * 3), cursor.i32("three"));
          }
        }
      });
    }
  }
}
