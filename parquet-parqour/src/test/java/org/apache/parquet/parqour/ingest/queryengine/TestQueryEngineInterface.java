package org.apache.parquet.parqour.ingest.queryengine;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestQueryEngineInterface extends UsesPersistence {
  private final int TOTAL_ROWS = TestTools.generateRandomInt(1000000);

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multipliers",
    new PrimitiveType(REQUIRED, INT32, "one"));

  public void generateTestData(ParquetConfiguration configuration) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, configuration.version(), 1, 10, configuration.useDictionary()) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group countingGroup = new SimpleGroup(COUNTING_SCHEMA)
            .append("one", index);

          writer.write(countingGroup);
        }
      }
    });
  }

  public TestQueryEngineInterface() {
    super(true);
  }

  @Test
  public void testSingleIntColumn() throws Exception {
    class Record {
      public final int one;
      public Record(Cursor cursor) {
        this.one = cursor.i32("one");
      }
    }

    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      this.generateTestData(configuration);

      TestTools.repeat(1, new TestTools.RepeatCallback() {
        @Override
        public void execute() throws Exception {
          int index = 0;

          // Set up the query.
          Parqour<Record> records = Parqour.query(TestTools.TEST_FILE_PATH)
            .materialize(new Projection<Cursor, Record>() {
              @Override
              public Record apply(Cursor cursor) {
                return new Record(cursor);
              }
            });

          // Iterate through the records.
          for (Record record : records) {
            assertEquals(index++, record.one);
          }

          assertEquals(TOTAL_ROWS, index);
        }
      });
    }
  }

}
