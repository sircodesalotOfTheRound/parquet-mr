package org.apache.parquet.parqour.ffreaders;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.manager.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.disk.pages.Page;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.plain.PlainSingleFastForwardReader;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/13/15.
 */
public class TestSingleFFReader extends UsesPersistence {
  private static int TOTAL = TestTools.generateRandomInt(10000);
  private static int ROW_TO_FAST_FORWARD_TO = TestTools.generateRandomInt(TOTAL);
  private static String COLUMN_NAME = "single";
  private static List<Float> NUMBERS = generateNumbers();

  private static List<Float> generateNumbers() {
    List<Float> numbers = new ArrayList<Float>();
    Random random = new Random(System.currentTimeMillis());
    for (int index = 0; index < TOTAL; index++) {
      numbers.add(random.nextFloat());
    }

    return numbers;
  }

  public static class SingleFloatWriteContext extends WriteTools.ParquetWriteContext {
    private static final MessageType SCHEMA = new MessageType("count",
      new PrimitiveType(REQUIRED, FLOAT, COLUMN_NAME));

    public SingleFloatWriteContext(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (float number : NUMBERS) {
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
      TestTools.generateTestData(new SingleFloatWriteContext(configuration));
      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(COLUMN_NAME).iterator().next();

      PlainSingleFastForwardReader reader = page.contentReader();

      int index = 0;
      while (!reader.isEof()) {
        assertEquals(NUMBERS.get(index), (Float)reader.readSingle());
        index++;
      }
    }
  }

  @Test
  public void testFastForwarding() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIG %s: TOTAL: %s, FAST-FORWARD-TO: %s", configuration, TOTAL, ROW_TO_FAST_FORWARD_TO);
      TestTools.generateTestData(new SingleFloatWriteContext(configuration));
      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(COLUMN_NAME).iterator().next();

      PlainSingleFastForwardReader reader = page.contentReader();

      reader.fastForwardTo(ROW_TO_FAST_FORWARD_TO);
      assertEquals(NUMBERS.get(ROW_TO_FAST_FORWARD_TO), (Float)reader.readSingle());
    }
  }
}
