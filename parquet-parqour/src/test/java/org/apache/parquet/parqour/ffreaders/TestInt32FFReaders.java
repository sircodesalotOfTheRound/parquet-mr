package org.apache.parquet.parqour.ffreaders;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageDecorator;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.UsesPersistence;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/13/15.
 */
public class TestInt32FFReaders extends UsesPersistence {
  private static int TOTAL = TestTools.generateRandomInt(50000);
  private static int INCREMENT = TestTools.generateRandomInt(10);
  private static String INCREMENT_COLUMN = "increment";

  public static class SingleIntegerColumnWriteContext extends WriteTools.ParquetWriteContext {
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "count",
      new PrimitiveType(REQUIRED, INT32, INCREMENT_COLUMN));

    public SingleIntegerColumnWriteContext(ParquetProperties.WriterVersion version) {
      super(SCHEMA, version, 1, 1, false);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL; index++) {
        SimpleGroup column = new SimpleGroup(SCHEMA);
        column.append(INCREMENT_COLUMN, index * INCREMENT);
        writer.write(column);
      }
    }
  }

  @Test
  public void testInt32FFReaderAgainstNoRLNoDLColumn() throws Exception {
    for (ParquetProperties.WriterVersion version : TestTools.PARQUET_VERSIONS) {
      TestTools.generateTestData(new SingleIntegerColumnWriteContext(version));

      QueryInfo queryInfo = TestTools.generateSchemaInfoFromPath(TestTools.TEST_FILE_PATH);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(queryInfo);
      ColumnDescriptor twiceIncrementColumn = queryInfo.getColumnDescriptorByPath(INCREMENT_COLUMN);
      DataPageDecorator page = diskInterfaceManager.getFirstPageForColumn(twiceIncrementColumn);
      Int32FastForwardReader reader = page.valuesReader();

      for (int index = 0; index < TOTAL; index++) {
        assertEquals(index * INCREMENT, reader.readi32());
      }
    }
  }

  private static int MODULUS = TestTools.generateRandomInt(20000);
  private static String MODULO_COLUMN = "moduloizedColumn";

  public static class SingleModuloizedIntWriterContext extends WriteTools.ParquetWriteContext {
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "count",
      new PrimitiveType(REQUIRED, INT32, MODULO_COLUMN));

    public SingleModuloizedIntWriterContext(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL; index++) {
        SimpleGroup column = new SimpleGroup(SCHEMA);
        column.append(MODULO_COLUMN, index + (index * index % MODULUS));
        writer.write(column);
      }
    }
  }

  @Test
  public void testDeltaReading() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.generateTestData(new SingleModuloizedIntWriterContext(configuration));

      QueryInfo queryInfo = TestTools.generateSchemaInfoFromPath(TestTools.TEST_FILE_PATH);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(queryInfo);
      ColumnDescriptor twiceIncrementColumn = queryInfo.getColumnDescriptorByPath(MODULO_COLUMN);
      DataPageDecorator page = diskInterfaceManager.getFirstPageForColumn(twiceIncrementColumn);
      Int32FastForwardReader ffReader = page.valuesReader();

      System.out.println(ffReader);
      System.out.println(String.format("%s:%s", TOTAL, MODULUS));
      for (int index = 0; index < TOTAL; index++) {
        assertEquals(index + (index * index % MODULUS), ffReader.readi32());
      }
    }
  }
}
