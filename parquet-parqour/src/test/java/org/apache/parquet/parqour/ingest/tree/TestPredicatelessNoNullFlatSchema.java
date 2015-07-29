package org.apache.parquet.parqour.ingest.tree;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.driver.ParqourPredicateReadDriver;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestPredicatelessNoNullFlatSchema extends UsesPersistence {
  private final int TOTAL_ROWS = TestTools.generateRandomInt(1000000);

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multipliers",
    new PrimitiveType(REQUIRED, INT32, "i32"),
    new PrimitiveType(REQUIRED, INT64, "i64"),
    new PrimitiveType(REQUIRED, BOOLEAN, "bool"),
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
            .append("i32", (int)(index))
            .append("i64", (long)(index * 2))
            .append("bool", (index % 3 == 0))
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
          QueryInfo queryInfo = new QueryInfo(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), metadata, COUNTING_SCHEMA);
          ParqourPredicateReadDriver driver = new ParqourPredicateReadDriver(queryInfo);

          Cursor cursor = driver.cursor();

          int index = 0;
          while (driver.readNext()) {
            assertEquals(index, (int) cursor.i32("i32"));
            assertEquals(index * 2, (long) cursor.i64("i64"));
            assertEquals((index % 3 == 0), cursor.bool("bool"));
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
