package org.apache.parquet.parqour.materialization;

import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.junit.Test;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.apache.parquet.parqour.testtools.TestTools.*;
import static org.junit.Assert.assertEquals;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestReadSupportMaterialization extends UsesPersistence {
  private final int TOTAL_ROWS = generateRandomInt(1000000);

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multipliers",
    new PrimitiveType(REQUIRED, INT32, "one"));

  public void generateTestData(ParquetConfiguration configuration) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, configuration.version(), 1, 10, configuration.useDictionary()) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group countingGroup = new SimpleGroup(COUNTING_SCHEMA);
          countingGroup.append("one", index);

          writer.write(countingGroup);
        }
      }
    });
  }

  public TestReadSupportMaterialization() {
    super(true);
  }

  @Test
  public void testIterator() throws Exception {
    for (ParquetConfiguration configuration : CONFIGURATIONS) {
      this.generateTestData(configuration);

      repeat(1, new RepeatCallback() {
        @Override
        public void execute() throws Exception {
          int index = 0;
          for (Group record : Parqour.query(TEST_FILE_PATH)
            .materialize(new GroupReadSupport())) {

            assertEquals(index, record.getInteger("one", 0));
            index++;
          }
        }
      });
    }
  }
}
