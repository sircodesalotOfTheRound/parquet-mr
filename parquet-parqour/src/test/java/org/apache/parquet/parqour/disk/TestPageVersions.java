package org.apache.parquet.parqour.disk;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupBlockInfo;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.DataPageInfo;
import org.apache.parquet.parqour.ingest.disk.pages.PageInfo;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 8/8/15.
 */
public class TestPageVersions {
  private final int TOTAL_ROWS = TestTools.generateRandomInt(10);
  private final MessageType SCHEMA = new MessageType("schema",
    new PrimitiveType(REQUIRED, INT32, "i32"),
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
          entry.append("float", (float)1.2345);
          entry.append("double", 2.46810);
          entry.append("binary", "something");
          entry.append("fixed-binary", "fiver");

          writer.write(entry);
        }
      }
    });
  }

  @Test
  public void testVersions() throws Exception {
    for (ParquetProperties.WriterVersion version : TestTools.PARQUET_VERSIONS) {
      this.generateTestData(version);

      try (HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH)) {
        HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
        for (RowGroupBlockInfo blockInfo : metadata.blocks()) {
          final Iterable<DataPageInfo> pages = blockInfo.columnMetadata()
            .map(new Projection<RowGroupColumnInfo, Iterable<PageInfo>>() {
              @Override
              public Iterable<PageInfo> apply(RowGroupColumnInfo columnInfo) {
                return columnInfo.pages();
              }
            })
            .flatten(new Projection<Iterable<PageInfo>, Iterable<PageInfo>>() {
              @Override
              public Iterable<PageInfo> apply(Iterable<PageInfo> pages) {
                return pages;
              }
            })
            .castTo(DataPageInfo.class);

          for (DataPageInfo pageInfo : pages) {
            assertEquals(version, pageInfo.version());
          }
        }
      }
    }
  }
}
