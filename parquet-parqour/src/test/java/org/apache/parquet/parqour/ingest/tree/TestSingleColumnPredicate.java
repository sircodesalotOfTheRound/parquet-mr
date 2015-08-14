package org.apache.parquet.parqour.ingest.tree;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager_OLD;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.impl.i32.Int32NoRepeatIngestNode;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Before;

import java.io.IOException;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.parqour.testtools.TestTools.EMPTY_CONFIGURATION;
import static org.apache.parquet.parqour.testtools.TestTools.TEST_FILE_PATH;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestSingleColumnPredicate extends UsesPersistence {
  private static final int TOTAL_ROWS = TestTools.generateRandomInt(1000000);
  private static int COMPARISON_VALUE = TestTools.generateRandomInt(TOTAL_ROWS);
  private static final String COUNT = "count";
  private static final Operators.IntColumn COLUMN = FilterApi.intColumn(COUNT);

  private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "count",
    new PrimitiveType(REQUIRED, INT32, COUNT));

  private static final Operators.Eq<Integer> EQUALS_PREDICATE = eq(COLUMN, COMPARISON_VALUE);
  private static final Operators.NotEq<Integer> NOT_EQUALS_PREDICATE = notEq(COLUMN, COMPARISON_VALUE);
  private static final Operators.Lt<Integer> LESS_THAN_PREDICATE = lt(COLUMN, COMPARISON_VALUE);
  private static final Operators.LtEq<Integer> LESS_THAN_OR_EQUALS_PREDICATE = ltEq(COLUMN, COMPARISON_VALUE);
  private static final Operators.Gt<Integer> GREATER_THAN_PREDICATE = gt(COLUMN, COMPARISON_VALUE);
  private static final Operators.GtEq<Integer> GREATER_THAN_OR_EQUALS_PREDICATE = gtEq(COLUMN, COMPARISON_VALUE);

  public void generateTestData(ParquetProperties.WriterVersion version) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(COUNTING_SCHEMA, version, 1, 10, false) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          writer.write(createGroupWithValue(index));
        }
      }

      public Group createGroupWithValue(int value) {
        return new SimpleGroup(COUNTING_SCHEMA).append(COUNT, value);
      }
    });
  }


  @Before
  public void generateTestData() {
    this.generateTestData(PARQUET_1_0);
  }

  // Testing based on row-index no longer makes sense if the blocks are pre-filtered.
/*
  @Test
  public void testEquality() throws Exception {
    this.runTest(EQUALS_PREDICATE, new ValidatePredicateCallback() {
      @Override
      public boolean validate(int rowIndex, PlainInt32IngestNode node) {
        if (rowIndex == COMPARISON_VALUE) {
          assertTrue(node.performPredicateTest(rowIndex));
          return true;
        } else {
          assertFalse(node.performPredicateTest(rowIndex));
          return false;
        }
      }
    });

    this.runTest(NOT_EQUALS_PREDICATE, new ValidatePredicateCallback() {
      @Override
      public boolean validate(int rowIndex, PlainInt32IngestNode node) {
        if (rowIndex != COMPARISON_VALUE) {
          assertTrue(node.performPredicateTest(rowIndex));
          return true;
        } else {
          assertFalse(node.performPredicateTest(rowIndex));
          return false;
        }
      }
    });
  }

  @Test
  public void testOrderdness() throws Exception {
    this.runTest(LESS_THAN_PREDICATE, new ValidatePredicateCallback() {
      @Override
      public boolean validate(int rowIndex, PlainInt32IngestNode node) {
        if (rowIndex < COMPARISON_VALUE) {
          assertTrue(node.performPredicateTest(rowIndex));
          return true;
        } else {
          assertFalse(node.performPredicateTest(rowIndex));
          return false;
        }
      }
    });

    this.runTest(LESS_THAN_OR_EQUALS_PREDICATE, new ValidatePredicateCallback() {
      @Override
      public boolean validate(int rowIndex, PlainInt32IngestNode node) {
        if (rowIndex <= COMPARISON_VALUE) {
          assertTrue(node.performPredicateTest(rowIndex));
          return true;
        } else {
          assertFalse(node.performPredicateTest(rowIndex));
          return false;
        }
      }
    });

    this.runTest(GREATER_THAN_PREDICATE, new ValidatePredicateCallback() {
      @Override
      public boolean validate(int rowIndex, PlainInt32IngestNode node) {
        if (rowIndex > COMPARISON_VALUE) {
          assertTrue(node.performPredicateTest(rowIndex));
          return true;
        } else {
          assertFalse(node.performPredicateTest(rowIndex));
          return false;
        }
      }
    });

    this.runTest(GREATER_THAN_OR_EQUALS_PREDICATE, new ValidatePredicateCallback() {
      @Override
      public boolean validate(int rowIndex, PlainInt32IngestNode node) {
        if (rowIndex >= COMPARISON_VALUE) {
          assertTrue(node.performPredicateTest(rowIndex));
          return true;
        } else {
          assertFalse(node.performPredicateTest(rowIndex));
          return false;
        }
      }
    });
  }
  */

  interface ValidatePredicateCallback { boolean validate(int rowIndex, Int32NoRepeatIngestNode node); }
  public void runTest(final FilterPredicate predicate, final ValidatePredicateCallback validationCallback) throws Exception {
    TestTools.repeat(1, new TestTools.RepeatCallback() {
      @Override
      public void execute() throws Exception {
        /*ParquetMetadata metadata = ParquetFileReader.readFooter(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), ParquetMetadataConverter.NO_FILTER);

        QueryInfo queryInfo = new QueryInfo(EMPTY_CONFIGURATION, new Path(TEST_FILE_PATH), metadata, COUNTING_SCHEMA, predicate);
        DiskInterfaceManager_OLD diskInterfaceManager = new DiskInterfaceManager_OLD(queryInfo);

        IngestTree ingest = new IngestTree(queryInfo, diskInterfaceManager);
        Int32NoRepeatIngestNode countNode = (Int32NoRepeatIngestNode) ingest.getIngestNodeByPath(COUNT);

        ingest.prepareForRead(TOTAL_ROWS);
        countNode.prepareForPredicateTesting();

        for (int index = 0; index < schemaInfo.totalRowCount(); index++) {
          countNode.prepareForPredicateRowTransaction();
          if (validationCallback.validate(index, countNode)) {
            countNode.commitPredicateTransaction();
          } else {
            countNode.rollbackPredicateTransaction();
          }
        }

        countNode.endPredicateTesting();
        ingest.endRead();*/
      }
    });
  }
}
