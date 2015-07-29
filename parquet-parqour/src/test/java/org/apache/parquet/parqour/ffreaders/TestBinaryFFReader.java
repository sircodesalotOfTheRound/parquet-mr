package org.apache.parquet.parqour.ffreaders;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/13/15.
 */
public class TestBinaryFFReader extends UsesPersistence {
  private static int TOTAL = 31000;
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
  public void testPlainBinaryFastForwardReader() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.generateTestData(new SingleBinaryColumnWriter(configuration));

      QueryInfo queryInfo = TestTools.generateSchemaInfoFromPath(TestTools.TEST_FILE_PATH);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(queryInfo);
      ColumnDescriptor doubleIncrementColumn = queryInfo.getColumnDescriptorByPath(COLUMN_NAME);
      DataPageDecorator page = diskInterfaceManager.getFirstPageForColumn(doubleIncrementColumn);
      BinaryFastForwardReader reader = page.valuesReader();

      System.out.println(reader);

      for (int index = 0; index < TOTAL; index++) {
        assertEquals(TestTools.FIRST_NAMES.getModulo(index), reader.readString());
      }
    }
  }
}
