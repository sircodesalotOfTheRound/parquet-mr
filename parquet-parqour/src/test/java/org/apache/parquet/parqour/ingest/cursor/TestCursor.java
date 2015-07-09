package org.apache.parquet.parqour.ingest.cursor;

import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.impl.Int32IngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.RootIngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

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
        ParquetMetadata metadata = ParquetFileReader.readFooter(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);

        SchemaInfo schemaInfo = new SchemaInfo(TestTools.EMPTY_CONFIGURATION, new Path(TestTools.TEST_FILE_PATH), metadata, COUNTING_SCHEMA);
        DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(schemaInfo);
        IngestTree ingestTree = new IngestTree(schemaInfo, diskInterfaceManager);

        RootIngestNode root = ingestTree.root();
        Int32IngestNode oneXIngest = (Int32IngestNode) ingestTree.getIngestNodeByPath(ONE_X);
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
