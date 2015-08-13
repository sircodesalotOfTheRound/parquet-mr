package org.apache.parquet.parqour.ffreaders;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.manager.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.disk.pages.Page;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BooleanFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/13/15.
 */
public class TestBooleanFFReaders extends UsesPersistence {
  private static int TOTAL = TestTools.generateRandomInt(50000);
  private static int ROW_TO_FAST_FORWARD_TO = TestTools.generateRandomInt(TOTAL);
  private static int MODULO = TestTools.generateRandomInt(1000) + 1;
  private static String COLUMN_NAME = "truefalse";

  public static class SingleBooleanWriteContext extends WriteTools.ParquetWriteContext {
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "count",
      new PrimitiveType(REQUIRED, BOOLEAN, COLUMN_NAME));

    public SingleBooleanWriteContext(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL; index++) {
        SimpleGroup column = new SimpleGroup(SCHEMA);
        column.append(COLUMN_NAME, index % MODULO == 0);
        writer.write(column);
      }
    }
  }

  @Test
  public void testNewReader() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIG %s: TOTAL: %s, MODULO: %s", configuration, TOTAL, MODULO);
      TestTools.generateTestData(new SingleBooleanWriteContext(configuration));
      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(COLUMN_NAME).iterator().next();

      BooleanFastForwardReader reader = page.contentReader();

      int index = 0;
      while (!reader.isEof()) {
        assertEquals((index % MODULO) == 0, reader.readtf());
        index++;
      }
    }
  }

  @Test
  public void testFastForwarding() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIG %s: TOTAL: %s, MODULO: %s, FAST-FORWARD-TO: %s",
        configuration, TOTAL, MODULO, ROW_TO_FAST_FORWARD_TO);
      TestTools.generateTestData(new SingleBooleanWriteContext(configuration));
      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(COLUMN_NAME).iterator().next();

      BooleanFastForwardReader reader = page.contentReader();

      reader.fastForwardTo(ROW_TO_FAST_FORWARD_TO);
      assertEquals(ROW_TO_FAST_FORWARD_TO % MODULO == 0, reader.readtf());
    }
  }
}
