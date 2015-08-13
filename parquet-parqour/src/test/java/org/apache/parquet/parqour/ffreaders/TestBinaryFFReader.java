package org.apache.parquet.parqour.ffreaders;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.manager.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.disk.pages.Page;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/13/15.
 */
public class TestBinaryFFReader extends UsesPersistence {
  private static int TOTAL = TestTools.generateRandomInt(31000);
  private static int ROW_TO_FAST_FORWARD_TO = TestTools.generateRandomInt(TOTAL);
  private static String COLUMN_NAME = "name";

  public static class SingleBinaryColumnWriter extends WriteTools.ParquetWriteContext {
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "names",
      new PrimitiveType(REQUIRED, BINARY, COLUMN_NAME));

    public SingleBinaryColumnWriter(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL; index++) {
        SimpleGroup column = new SimpleGroup(SCHEMA);
        column.append(COLUMN_NAME, TestTools.FIRST_NAMES.getModulo(index));
        writer.write(column);
      }
    }
  }

  @Test
  public void testReaders() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIG %s: TOTAL: %s", configuration, TOTAL);
      TestTools.generateTestData(new SingleBinaryColumnWriter(configuration));
      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(COLUMN_NAME).iterator().next();

      BinaryFastForwardReader reader = page.contentReader();

      int index = 0;
      while (!reader.isEof()) {
        assertEquals(TestTools.FIRST_NAMES.getModulo(index), reader.readString());
        index++;
      }
    }
  }

  @Test
  public void testFastForwarding() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIG %s: TOTAL: %s, FAST-FORWARD-TO: %s",
        configuration, TOTAL, ROW_TO_FAST_FORWARD_TO);
      TestTools.generateTestData(new SingleBinaryColumnWriter(configuration));
      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(COLUMN_NAME).iterator().next();

      BinaryFastForwardReader reader = page.contentReader();

      reader.fastForwardTo(ROW_TO_FAST_FORWARD_TO);
      assertEquals(TestTools.FIRST_NAMES.getModulo(ROW_TO_FAST_FORWARD_TO), reader.readString());
    }
  }
}
