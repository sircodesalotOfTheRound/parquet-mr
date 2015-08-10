package org.apache.parquet.parqour.disk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Created by sircodesalot on 8/8/15.
 */
public class TestPageVersions {
  private final MessageType PERSON_SCHEMA = new MessageType("person",
    new PrimitiveType(OPTIONAL, INT32, "id"),
    new PrimitiveType(OPTIONAL, INT64, "second"));

  public void generateTestData(ParquetProperties.WriterVersion version) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(PERSON_SCHEMA, version, 1, 10, false) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < 10000000; index++) {
          Group person = new SimpleGroup(PERSON_SCHEMA);
          person.append("id", 1);
          person.append("second", (long) 2);

          writer.write(person);
        }
      }
    });
  }

  // Broken test.
  @Test
  public void testDiskInterfaceManager() throws Exception {
    //for (ParquetProperties.WriterVersion version : TestTools.PARQUET_VERSIONS) {
    this.generateTestData(ParquetProperties.WriterVersion.PARQUET_2_0);

    Configuration configuration = new Configuration();
    Path path = new Path(TestTools.TEST_FILE_PATH);
    ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, path);
    for (BlockMetaData block : metadata.getBlocks()) {
      ColumnChunkMetaData id = block.getColumns().get(0);
      ColumnChunkMetaData second = block.getColumns().get(1);

      block.getStartingPos();
      FileSystem fileSystem = FileSystem.get(configuration);
      FSDataInputStream file = fileSystem.open(path);

      file.seek(4);

      PageHeader pageHeader = Util.readPageHeader(file);
      System.out.println(pageHeader);

    }
    //}
  }
}
