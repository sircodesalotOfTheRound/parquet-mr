package org.apache.parquet.parqour.ingest.paging;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.ingest.read.iterator.paging.ParqourPage;
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
import static org.junit.Assert.assertTrue;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestPaging extends UsesPersistence {
  private final int TOTAL_ROWS = 1000000;
  private final int PAGE_SIZE = TestTools.generateRandomInt(10000);

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "table",
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

  public TestPaging() {
    super(true);
  }

  @Test
  public void testPagingOnASingleColumn() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      this.generateTestData(configuration);
      System.out.println(PAGE_SIZE);

      TestTools.repeat(1, new TestTools.RepeatCallback() {
        @Override
        public void execute() throws Exception {
          int totalRead = 0;
          for (ParqourPage<Cursor> page : Parqour.query(TestTools.TEST_FILE_PATH).paginate(PAGE_SIZE)) {
            int pageItemCount = 0;
            for (Cursor cursor : page) {
              assertEquals(totalRead++, (int)cursor.i32("one"));
              pageItemCount++;
            }

            assertTrue(pageItemCount <= PAGE_SIZE);
          }

          assertEquals(TOTAL_ROWS, totalRead);
        }
      });
    }
  }
}
