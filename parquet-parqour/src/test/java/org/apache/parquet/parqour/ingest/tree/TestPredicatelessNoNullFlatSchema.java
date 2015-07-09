package org.apache.parquet.parqour.ingest.tree;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.driver.ParqourReadDriver;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestPredicatelessNoNullFlatSchema extends UsesPersistence {
  private final int TOTAL_ROWS = TestTools.generateRandomInt(1000000);

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multipliers",
    new PrimitiveType(REQUIRED, INT32, "one"),
    new PrimitiveType(REQUIRED, INT32, "two"),
    new PrimitiveType(REQUIRED, INT32, "three"),
    new PrimitiveType(REQUIRED, INT32, "four"),
    new PrimitiveType(REQUIRED, INT32, "five"),
    new PrimitiveType(REQUIRED, INT32, "six"),
    new PrimitiveType(REQUIRED, INT32, "seven"),
    new PrimitiveType(REQUIRED, INT32, "eight"),
    new PrimitiveType(REQUIRED, INT32, "nine"),
    new PrimitiveType(REQUIRED, INT32, "ten"));

  public void generateTestData(ParquetConfiguration configuration) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, configuration.version(), 1, 10, configuration.useDictionary()) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group countingGroup = new SimpleGroup(COUNTING_SCHEMA)
            .append("one", index)
            .append("two", index * 2)
            .append("three", index * 3)
            .append("four", index * 4)
            .append("five", index * 5)
            .append("six", index * 6)
            .append("seven", index * 7)
            .append("eight", index * 8)
            .append("nine", index * 9)
            .append("ten", index * 10);

          writer.write(countingGroup);
        }
      }
    });
  }

  public TestPredicatelessNoNullFlatSchema() {
    super(true);
  }

  @Test
  public void testThreeFlatColumnPredicatelessSchema() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      this.generateTestData(configuration);

      TestTools.repeat(1, new TestTools.RepeatCallback() {
        @Override
        public void execute() throws Exception {
          ParquetMetadata metadata = ParquetFileReader.readFooter(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);
          SchemaInfo schemaInfo = new SchemaInfo(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), metadata, COUNTING_SCHEMA);
          ParqourReadDriver driver = new ParqourReadDriver(schemaInfo);

          Cursor cursor = driver.cursor();

          int index = 0;
          while (driver.readNext()) {
            assertEquals(index, (int) cursor.i32("one"));
            assertEquals(index * 2, (int) cursor.i32("two"));
            assertEquals(index * 3, (int) cursor.i32("three"));
            assertEquals(index * 4, (int) cursor.i32("four"));
            assertEquals(index * 5, (int) cursor.i32("five"));
            assertEquals(index * 6, (int) cursor.i32("six"));
            assertEquals(index * 7, (int) cursor.i32("seven"));
            assertEquals(index * 8, (int) cursor.i32("eight"));
            assertEquals(index * 9, (int) cursor.i32("nine"));
            assertEquals(index * 10, (int) cursor.i32("ten"));

            index++;
          }
        }
      });
    }
  }
}
