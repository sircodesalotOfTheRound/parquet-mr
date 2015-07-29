package org.apache.parquet.parqour.ingest.tree;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.driver.ParqourPredicateReadDriver;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestPredicatelessTwoColumnAggregation extends UsesPersistence {
  private final int TOTAL_ROWS = TestTools.generateRandomInt(50000);
  private static final String ONE_X = "onex";
  private static final String TWO_X = "twox";

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multiplierAggregate",
    new GroupType(REQUIRED, "pair",
      new PrimitiveType(REQUIRED, INT32, ONE_X),
      new PrimitiveType(REQUIRED, INT32, TWO_X)));

  public void generateTestData(ParquetProperties.WriterVersion version) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, version, 1, 10, false) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group countingGroup = new SimpleGroup(COUNTING_SCHEMA);
          countingGroup.addGroup("pair")
            .append(ONE_X, index)
            .append(TWO_X, index * 2);

          writer.write(countingGroup);
        }
      }
    });
  }

  @Test
  public void testSimpleAggregation() throws Exception {
    this.generateTestData(PARQUET_1_0);

    TestTools.repeat(1, new TestTools.RepeatCallback() {
      @Override
      public void execute() throws Exception {
        ParquetMetadata metadata = ParquetFileReader.readFooter(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);

        QueryInfo queryInfo = new QueryInfo(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), metadata, COUNTING_SCHEMA);
        ParqourPredicateReadDriver driver = new ParqourPredicateReadDriver(queryInfo);

        // Follow the aggregate pointers to the leaf nodes.
        Cursor cursor = driver.cursor();

        int index = 0;
        while (driver.readNext()) {
          assertEquals(index, (int)cursor.field("pair").i32("onex"));
          assertEquals(index * 2, (int)cursor.field("pair").i32("twox"));
          index++;
        }
      }
    });
  }
}
