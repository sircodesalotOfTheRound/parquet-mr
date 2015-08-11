package org.apache.parquet.parqour.ingest.tree;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestScratch extends UsesPersistence {
  private final int TOTAL_ROWS = 1000000;
  private final int ROW_TO_SAERCH_FOR = 999991;

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

  public void generateTestData(ParquetProperties.WriterVersion version) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, version, 1, 10, false) {
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
            .append("ten", index);

          writer.write(countingGroup);
        }
      }
    });
  }

  public TestScratch() {
    super(true);
  }

  public void testThreeFlatColumnPredicatelessSchema() throws Exception {

    this.generateTestData(ParquetProperties.WriterVersion.PARQUET_1_0);

    for (Handler handler : Logger.getLogger("").getHandlers()) {
      Logger.getLogger("").removeHandler(handler);
      handler.close();
    }
    LogManager.getLogManager().reset();

    TestTools.repeat(10000, new TestTools.RepeatCallback() {
      @Override
      public void execute() throws Exception {
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(TestTools.TEST_FILE_PATH)).withConf(TestTools.EMPTY_CONFIGURATION)
          //.withFilter(FilterCompat.get(FilterApi.eq(FilterApi.intColumn("ten"), ROW_TO_SAERCH_FOR)))
          .build();
        //for (int index = 0; index < TOTAL_ROWS; index++) {
        //for (int index = 0; index < TOTAL_ROWS; index++) {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group group = reader.read();
          group.getInteger("one", 0);
          group.getInteger("two", 0);
          group.getInteger("three", 0);
          group.getInteger("four", 0);
          group.getInteger("five", 0);
          group.getInteger("six", 0);
          group.getInteger("seven", 0);
          group.getInteger("eight", 0);
          group.getInteger("nine", 0);
          group.getInteger("ten", 0);
        }
        //System.err.println("found " + group.getInteger(0, 0));
        //}

//          assertEquals(ROW_TO_SAERCH_FOR, group.getInteger("one", 0));
          /*assertEquals(index * 2, group.getInteger("two", 0));
          assertEquals(index * 3, group.getInteger("three", 0));
          assertEquals(index * 4, group.getInteger("four", 0));
          assertEquals(index * 5, group.getInteger("five", 0));
          assertEquals(index * 6, group.getInteger("six", 0));
          assertEquals(index * 7, group.getInteger("seven", 0));
          assertEquals(index * 8, group.getInteger("eight", 0));
          assertEquals(index * 9, group.getInteger("nine", 0));
          assertEquals(index * 10, group.getInteger("ten", 0));*/

        reader.close();
        /*ParquetMetadata metadata = ParquetFileReader.readFooter(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);
        SchemaInfo schemaInfo = new SchemaInfo(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), metadata, COUNTING_SCHEMA);
        DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(schemaInfo);


        DataPageDecorator page = diskInterfaceManager.getFirstPageForColumn(new ColumnDescriptor(new String[]{"one"}, PrimitiveType.PrimitiveTypeName.INT32, 0, 0));
        for (int index = 0; index < 38; index++) {
          page = diskInterfaceManager.getNextPageForColumn(page);
        }*/
/*
        IngestTree ingestTree = new IngestTree(schemaInfo, diskInterfaceManager);

        PlainInt32IngestNode oneXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("one");
        PlainInt32IngestNode twoXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("two");
        PlainInt32IngestNode threeXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("three");
        PlainInt32IngestNode fourXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("four");
        PlainInt32IngestNode fiveXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("five");
        PlainInt32IngestNode sixXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("six");
        PlainInt32IngestNode sevenXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("seven");
        PlainInt32IngestNode eightXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("eight");
        PlainInt32IngestNode nineXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("nine");
        PlainInt32IngestNode tenXIngest = (PlainInt32IngestNode) ingestTree.getIngestNodeByPath("ten");

        assertEquals(oneXIngest.repetitionType(), REQUIRED);
        assertEquals(twoXIngest.repetitionType(), REQUIRED);
        assertEquals(threeXIngest.repetitionType(), REQUIRED);
        assertEquals(fourXIngest.repetitionType(), REQUIRED);
        assertEquals(fiveXIngest.repetitionType(), REQUIRED);
        assertEquals(sixXIngest.repetitionType(), REQUIRED);
        assertEquals(sevenXIngest.repetitionType(), REQUIRED);
        assertEquals(eightXIngest.repetitionType(), REQUIRED);
        assertEquals(nineXIngest.repetitionType(), REQUIRED);

        assertEquals(oneXIngest.repetitionLevelAtThisNode(), 1);
        assertEquals(twoXIngest.repetitionLevelAtThisNode(), 1);
        assertEquals(threeXIngest.repetitionLevelAtThisNode(), 1);

        assertEquals(0, oneXIngest.definitionLevelAtThisNode());
        assertEquals(0, twoXIngest.definitionLevelAtThisNode());
        assertEquals(0, threeXIngest.definitionLevelAtThisNode());

        ingestTree.prepareForRead(TOTAL_ROWS);

        oneXIngest.prepareForContinuousRead();
        twoXIngest.prepareForContinuousRead();
        threeXIngest.prepareForContinuousRead();
        fourXIngest.prepareForContinuousRead();
        fiveXIngest.prepareForContinuousRead();
        sixXIngest.prepareForContinuousRead();
        sevenXIngest.prepareForContinuousRead();
        eightXIngest.prepareForContinuousRead();
        nineXIngest.prepareForContinuousRead();
        tenXIngest.prepareForContinuousRead();

        oneXIngest.performContinuousRead(readRows);
        twoXIngest.performContinuousRead(readRows);
        threeXIngest.performContinuousRead(readRows);
        fourXIngest.performContinuousRead(readRows);
        fiveXIngest.performContinuousRead(readRows);
        sixXIngest.performContinuousRead(readRows);
        sevenXIngest.performContinuousRead(readRows);
        eightXIngest.performContinuousRead(readRows);
        nineXIngest.performContinuousRead(readRows);
        tenXIngest.performContinuousRead(readRows);

        oneXIngest.endContinousRead();
        twoXIngest.endContinousRead();
        threeXIngest.endContinousRead();
        fourXIngest.endContinousRead();
        fiveXIngest.endContinousRead();
        sixXIngest.endContinousRead();
        sevenXIngest.endContinousRead();
        eightXIngest.endContinousRead();
        nineXIngest.endContinousRead();
        tenXIngest.endContinousRead();

        ingestTree.endRead();

        GroupAggregateCursor cursor = ingestTree.root().collectAggregate();

        assertTrue(cursor.allResultsReported());

        for (int index = 0; index < TOTAL_ROWS; index++) {
          cursor.advanceTo(index);

          assertEquals(index, cursor.i32("one"));
          assertEquals(index * 2, cursor.i32("two"));
          assertEquals(index * 3, cursor.i32("three"));
          assertEquals(index * 4, cursor.i32("four"));
          assertEquals(index * 5, cursor.i32("five"));
          assertEquals(index * 6, cursor.i32("six"));
          assertEquals(index * 7, cursor.i32("seven"));
          assertEquals(index * 8, cursor.i32("eight"));
          assertEquals(index * 9, cursor.i32("nine"));
          assertEquals(index * 10, cursor.i32("ten"));
        }*/
      }
    });
  }
}