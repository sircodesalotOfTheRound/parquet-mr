package org.apache.parquet.parqour.ingest.driver;

import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/16/15.
 */
public class TestPredicateDriver {
  private static int TOTAL_ROWS = 1000000;
  private static int ROW_TO_SEARCH_FOR = TestTools.generateRandomInt(TOTAL_ROWS);
  private static final Operators.IntColumn COLUMN = FilterApi.intColumn("one");
  private static final Operators.Eq<Integer> EQUALS_PREDICATE = FilterApi.eq(COLUMN, ROW_TO_SEARCH_FOR);

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multipliers",
    new PrimitiveType(REQUIRED, INT32, "one"),
    new PrimitiveType(REQUIRED, INT32, "two"),
    new PrimitiveType(REQUIRED, INT32, "three"));

  public void generateTestData(ParquetProperties.WriterVersion version) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, version, 1, 10, false) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group countingGroup = new SimpleGroup(COUNTING_SCHEMA)
            .append("one", index)
            .append("two", index * 2)
            .append("three", index * 3);

          writer.write(countingGroup);
        }
      }
    });
  }

  /*
  @Test
  public void testReadDriver() throws Exception {
    this.generateTestData(PARQUET_1_0);

    TestTools.repeat(1, new TestTools.RepeatCallback() {
      @Override
      public void execute() throws Exception {
        ParquetMetadata metadata = ParquetFileReader.readFooter(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);
        SchemaInfo schemaInfo = new SchemaInfo(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), metadata, COUNTING_SCHEMA, EQUALS_PREDICATE);
        DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(schemaInfo);

        IngestTree ingestTree = new IngestTree(schemaInfo, diskInterfaceManager);
        ParquetAdvancedReadDriver driver = new ParquetAdvancedReadDriver(ingestTree.schema());
        driver.readRowBatch((int)schemaInfo.totalRowCount());

        // Should have all results reported, and there should be just one item.
        GroupAggregateCursor aggregate = driver.collectAggregate();
        assertTrue(aggregate.allResultsReported());
        assertEquals(1, aggregate.maxChildColumnSize());

        Cursor columnOne = aggregate.getResultSetForColumn(0);
        Cursor columnTwo = aggregate.getResultSetForColumn(1);
        Cursor columnThree = aggregate.getResultSetForColumn(2);

        assertEquals(ROW_TO_SEARCH_FOR, columnOne.i32());
        assertEquals(ROW_TO_SEARCH_FOR * 2, columnTwo.i32());
        assertEquals(ROW_TO_SEARCH_FOR * 3, columnThree.i32());
      }
    });
  }*/
}
