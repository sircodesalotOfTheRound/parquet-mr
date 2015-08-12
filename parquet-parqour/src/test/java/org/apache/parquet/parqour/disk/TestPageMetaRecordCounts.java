package org.apache.parquet.parqour.disk;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.manager.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.disk.pages.Page;
import org.apache.parquet.parqour.ingest.disk.pages.Pager;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 8/8/15.
 */
public class TestPageMetaRecordCounts {
  private final int TOTAL_ROWS = TestTools.generateRandomInt(1000000);
  private final MessageType SCHEMA = new MessageType("schema",
    new PrimitiveType(OPTIONAL, INT32, "i32"),
    new PrimitiveType(OPTIONAL, INT64, "i64"),
    new PrimitiveType(OPTIONAL, INT96, "i96"),
    new PrimitiveType(OPTIONAL, BOOLEAN, "boolean"),
    new PrimitiveType(OPTIONAL, FLOAT, "float"),
    new PrimitiveType(OPTIONAL, DOUBLE, "double"),
    new PrimitiveType(OPTIONAL, BINARY, "binary"),
    new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 5, "fixed-binary"));

  public void generateTestData(ParquetProperties.WriterVersion version) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(SCHEMA, version, 1, 10, false) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group entry = new SimpleGroup(SCHEMA);
          entry.append("i32", 1);
          entry.append("i64", (long) 2);
          entry.append("i96", Binary.fromConstantByteArray(new byte[12]));
          entry.append("boolean", true);
          entry.append("float", (float) 1.2345);
          entry.append("double", 2.46810);
          entry.append("binary", "something");
          entry.append("fixed-binary", "fiver");

          writer.write(entry);
        }
      }
    });
  }

  @Test
  public void testPagination() throws Exception {
    for (ParquetProperties.WriterVersion version : TestTools.PARQUET_VERSIONS) {
      this.generateTestData(version);
      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);

      assertAllEntriesAccountedFor(diskInterfaceManager.pagerFor("i32"), TOTAL_ROWS);
      assertAllEntriesAccountedFor(diskInterfaceManager.pagerFor("i64"), TOTAL_ROWS);
      assertAllEntriesAccountedFor(diskInterfaceManager.pagerFor("i96"), TOTAL_ROWS);
      assertAllEntriesAccountedFor(diskInterfaceManager.pagerFor("boolean"), TOTAL_ROWS);
      assertAllEntriesAccountedFor(diskInterfaceManager.pagerFor("float"), TOTAL_ROWS);
      assertAllEntriesAccountedFor(diskInterfaceManager.pagerFor("double"), TOTAL_ROWS);
      assertAllEntriesAccountedFor(diskInterfaceManager.pagerFor("binary"), TOTAL_ROWS);
      assertAllEntriesAccountedFor(diskInterfaceManager.pagerFor("fixed-binary"), TOTAL_ROWS);
    }
  }

  private void assertAllEntriesAccountedFor(Pager pager, int total) {
    for (Page page : pager) {
      total -= page.totalEntries();
    }

    assertEquals(0, total);
  }
}
