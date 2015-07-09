package org.apache.parquet.parqour.disk;

import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageDecorator;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.parqour.testtools.TestTools.PARQUET_VERSIONS;
import static org.apache.parquet.parqour.testtools.TestTools.TEST_FILE_PATH;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class TestDiskInterfaceManager extends UsesPersistence {
  private static String ID = "id";
  private static String AGE = "age";
  private static String FIRST = "first";
  private static String LAST = "last";


  private final GroupType PERSON_SCHEMA = new GroupType(REQUIRED, "person",
    new PrimitiveType(REQUIRED, INT32, "id"),
    new PrimitiveType(REQUIRED, INT32, "age"),
    new PrimitiveType(REQUIRED, BINARY, "first"),
    new PrimitiveType(REQUIRED, BINARY, "last"));

  public void generateTestData(ParquetProperties.WriterVersion version) {
    WriteTools.withParquetWriter(new WriteTools.ParquetWriteContext(PERSON_SCHEMA, version, 1, 10, false) {
      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        Group person = new SimpleGroup(PERSON_SCHEMA)
          .append(ID, 2)
          .append(AGE, 15)
          .append(FIRST, "john")
          .append(LAST, "hancock");

        writer.write(person);
      }
    });
  }

  @Test
  public void testDiskInterfaceManager() throws Exception {
    for (ParquetProperties.WriterVersion version : PARQUET_VERSIONS) {
      this.generateTestData(version);

      SchemaInfo schemaInfo = TestTools.generateSchemaInfoFromPath(TEST_FILE_PATH);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(schemaInfo);

      Int32FastForwardReader idReader = generateReaderForColumn(ID, schemaInfo, diskInterfaceManager);
      Int32FastForwardReader ageReader = generateReaderForColumn(AGE, schemaInfo, diskInterfaceManager);
      BinaryFastForwardReader firstReader = generateReaderForColumn(FIRST, schemaInfo, diskInterfaceManager);
      BinaryFastForwardReader lastReader = generateReaderForColumn(LAST, schemaInfo, diskInterfaceManager);

      // Should be one item on the page, and that value should be two.
      assertEquals(1, idReader.totalItemsOnPage());
      assertEquals(1, ageReader.totalItemsOnPage());
      assertEquals(1, firstReader.totalItemsOnPage());
      assertEquals(1, lastReader.totalItemsOnPage());

      assertEquals(2, idReader.readi32());
      assertEquals(15, ageReader.readi32());

      System.out.println(lastReader);
      assertEquals("john", firstReader.readString());
      assertEquals("hancock", new String(lastReader.readBytes()));
    }

  }

  private <T extends FastForwardReaderBase> T generateReaderForColumn(String columnName, SchemaInfo schemaInfo, DiskInterfaceManager diskInterfaceManager) {
    ColumnDescriptor columnDescriptor = schemaInfo.getColumnDescriptorByPath(columnName);
    DataPageDecorator page = diskInterfaceManager.getFirstPageForColumn(columnDescriptor);

    return page.valuesReader();
  }
}
