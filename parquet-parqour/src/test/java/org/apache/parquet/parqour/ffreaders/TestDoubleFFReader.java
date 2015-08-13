package org.apache.parquet.parqour.ffreaders;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.manager.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.disk.pages.Page;
import org.apache.parquet.parqour.ingest.ffreader.plain.PlainDoubleFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.plain.PlainSingleFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageDecorator;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager_OLD;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/13/15.
 */
public class TestDoubleFFReader extends UsesPersistence {
  private static int TOTAL = TestTools.generateRandomInt(1000);
  private static int ROW_TO_FAST_FORWARD_TO = TestTools.generateRandomInt(TOTAL);
  private static String COLUMN_NAME = "double";
  private static List<Double> NUMBERS = generateNumbers();

  private static List<Double> generateNumbers() {
    List<Double> numbers = new ArrayList<Double>();
    Random random = new Random(System.currentTimeMillis());
    for (int index = 0; index < TOTAL; index++) {
      numbers.add(random.nextDouble());
    }

    return numbers;
  }

  public static class SingleDoubleWriteContext extends WriteTools.ParquetWriteContext {
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "count",
      new PrimitiveType(REQUIRED, DOUBLE, COLUMN_NAME));

    public SingleDoubleWriteContext(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (Double number : NUMBERS) {
        SimpleGroup column = new SimpleGroup(SCHEMA);
        column.append(COLUMN_NAME, number);
        writer.write(column);
      }
    }
  }

  @Test
  public void testReaders() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIG %s: TOTAL: %s", configuration, TOTAL);
      TestTools.generateTestData(new SingleDoubleWriteContext(configuration));
      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(COLUMN_NAME).iterator().next();

      PlainDoubleFastForwardReader reader = page.contentReader();

      int index = 0;
      while (!reader.isEof()) {
        assertEquals(NUMBERS.get(index), reader.readDouble());
        index++;
      }
    }
  }

  @Test
  public void testFastForwarding() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIG %s: TOTAL: %s, FAST-FORWARD-TO: %s", configuration, TOTAL, ROW_TO_FAST_FORWARD_TO);
      TestTools.generateTestData(new SingleDoubleWriteContext(configuration));
      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(COLUMN_NAME).iterator().next();

      PlainDoubleFastForwardReader reader = page.contentReader();

      reader.fastForwardTo(ROW_TO_FAST_FORWARD_TO);
      assertEquals(NUMBERS.get(ROW_TO_FAST_FORWARD_TO), reader.readDouble());
    }
  }
}
