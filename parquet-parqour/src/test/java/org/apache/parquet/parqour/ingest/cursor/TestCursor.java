package org.apache.parquet.parqour.ingest.cursor;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/18/15.
 */
public class TestCursor {
    private final int TOTAL_ROWS = TestTools.generateRandomInt(50000);
  private static final String ONE_X = "onex";
  private static final String TWO_X = "twox";

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multiplierAggregate",
    new PrimitiveType(REQUIRED, INT32, ONE_X));

  public void generateTestData(ParquetProperties.WriterVersion version) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, version, 1, 10, false) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group countingGroup = new SimpleGroup(COUNTING_SCHEMA)
            .append(ONE_X, index);

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
        /*ParquetMetadata metadata = ParquetFileReader.readFooter(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);

        QueryInfo queryInfo = new QueryInfo(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), metadata, COUNTING_SCHEMA);
        DiskInterfaceManager_OLD diskInterfaceManager = new DiskInterfaceManager_OLD(queryInfo);
        IngestTree ingestTree = new IngestTree(queryInfo, diskInterfaceManager);

        RootIngestNode root = ingestTree.root();
        Int32NoRepeatIngestNode oneXIngest = (Int32NoRepeatIngestNode) ingestTree.getIngestNodeByPath(ONE_X);
/*
        root.prepareForReading(TOTAL_ROWS);
        oneXIngest.prepareForContinuousRead();
        oneXIngest.performContinuousRead(rowSet);
        oneXIngest.endContinousRead();
        oneXIngest.endReading();

        AdvanceableCursor cursor = root.collectAggregate();
        for (int index = 0; index < TOTAL_ROWS; index++) {
          cursor.advanceTo(index);
          assertEquals(index, cursor.i32(ONE_X));
        }*/
      }
    });
  }

  @Test
  public void testCursor() {

  }
}
