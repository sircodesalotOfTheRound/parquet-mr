package org.apache.parquet.parqour.ingest.driver;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.ParqourRecordset;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.apache.parquet.parqour.testtools.TestTools.EMPTY_CONFIGURATION;
import static org.apache.parquet.parqour.testtools.TestTools.TEST_FILE_PATH;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/22/15.
 */
public class TestListOfLists {
  private static int TOTAL_ROWS = TestTools.generateRandomInt(1000000);

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "schema",
    new GroupType(REPEATED, "grouping",
      new PrimitiveType(REPEATED, INT32, "first"),
      new PrimitiveType(REPEATED, INT32, "second")));

  public void generateTestData(ParquetConfiguration configuration) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, configuration.version(), 1, 10, configuration.useDictionary()) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group countingGroup = new SimpleGroup(COUNTING_SCHEMA);

          for (int groupRepeat = 0; groupRepeat < (index % 7) + 1; groupRepeat++) {
            Group subgroup = countingGroup.addGroup("grouping");
            for (int firstRepeat = 0; firstRepeat < (index % 3) + 1; firstRepeat++) {
              subgroup.append("first", index + groupRepeat + firstRepeat);
            }
            for (int secondRepeat = 0; secondRepeat < (index % 5) + 1; secondRepeat++) {
              subgroup.append("second", index * 2 + groupRepeat + secondRepeat);
            }
          }

          writer.write(countingGroup);
        }
      }
    });
  }

  //@Test
  public void testVaryingRepeatLevels() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      this.generateTestData(configuration);

      TestTools.repeat(1, new TestTools.RepeatCallback() {
        @Override
        public void execute() throws Exception {
          ParquetMetadata metadata = ParquetFileReader.readFooter(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);
          SchemaInfo schemaInfo = new SchemaInfo(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), metadata, COUNTING_SCHEMA);

          int totalRepeat = 0;
          for (Cursor cursor : new ParqourRecordset(schemaInfo)) {
            int groupRepeat = 0;
            for (Cursor grouping : cursor.fieldIter(0)) {

              int firstRepeat = 0;
              for (int value : grouping.i32Iter(0)) {
                assertEquals(totalRepeat + groupRepeat + firstRepeat, value);
                firstRepeat++;
              }
              assertEquals(totalRepeat % 3 + 1, firstRepeat);

              int secondRepeat= 0;
              for (int value : grouping.i32Iter(1)) {
                assertEquals((totalRepeat * 2) + groupRepeat + secondRepeat, value);
                secondRepeat++;
              }
              assertEquals(totalRepeat % 5 + 1, secondRepeat);

              groupRepeat++;
            }

            assertEquals(totalRepeat % 7 + 1, groupRepeat);
            totalRepeat++;
          }

        }
      });
    }
  }
}
