package org.apache.parquet.parqour.materialization;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.ingest.read.iterator.ParqourRecordset;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.parqour.testtools.TestTools.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestLambdaMaterialization extends UsesPersistence {
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

  public TestLambdaMaterialization() {
    super(true);
  }

  @Test
  public void testIterator() throws Exception {
    for (ParquetConfiguration configuration : CONFIGURATIONS) {
      this.generateTestData(configuration);

      repeat(1, new RepeatCallback() {
        @Override
        public void execute() throws Exception {
          ParquetMetadata metadata = ParquetFileReader.readFooter(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);
          QueryInfo queryInfo = new QueryInfo(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), metadata, COUNTING_SCHEMA);

          Parqour<Record> records = new ParqourRecordset(queryInfo)
            //.materialize(Record::new) Java-8 style lambda
            .materialize(new Projection<Cursor, Record>() {
              @Override
              public Record apply(Cursor cursor) {
                return new Record(cursor);
              }
            });

          int index = 0;
          for (Record record : records) {
            assertEquals(index, record.one());
            index++;
          }
        }
      });
    }
  }


  public static class Record {
    private final int one;

    public Record(Record record) {
      this.one = record.one();
    }

    public Record(Cursor cursor) {
      this.one = cursor.i32("one");
    }

    public int one() { return this.one; }
  }
}
