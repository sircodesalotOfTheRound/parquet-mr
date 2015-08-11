package org.apache.parquet.parqour.ffreaders;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/13/15.
 */
public class TestPlainFixedBinaryFFReader extends UsesPersistence {
  private static int TOTAL = TestTools.generateRandomInt(50000);
  private static String COLUMN_NAME = "name";
  private static int FIELD_LENGTH = 12;

  private static List<String> NAMES = new ArrayList<String>() {{
    add("sammy       ");
    add("louis       ");
    add("samantha    ");
    add("rob         ");
    add("juliange    ");
    add("siddartha   ");
    add("jose        ");
    add("rebecca     ");
    add("thomas      ");
    add("jean patrick");
  }};

  public static class SingleNameWriteContext extends WriteTools.ParquetWriteContext {
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "names",
      new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, FIELD_LENGTH, COLUMN_NAME));

    public SingleNameWriteContext(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL; index++) {
        SimpleGroup column = new SimpleGroup(SCHEMA);
        column.append(COLUMN_NAME, NAMES.get(index % NAMES.size()));
        writer.write(column);
      }
    }
  }

  @Test
  public void testIntegerFFReaderAgainstNoRLNoDLColumn() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.generateTestData(new SingleNameWriteContext(configuration));

      QueryInfo queryInfo = TestTools.generateSchemaInfoFromPath(TestTools.TEST_FILE_PATH);
      DiskInterfaceManager_OLD diskInterfaceManager = new DiskInterfaceManager_OLD(queryInfo);
      ColumnDescriptor doubleIncrementColumn = queryInfo.getColumnDescriptorByPath(COLUMN_NAME);
      DataPageDecorator page = diskInterfaceManager.getFirstPageForColumn(doubleIncrementColumn);
      BinaryFastForwardReader reader = page.valuesReader();

      for (int index = 0; index < TOTAL; index++) {
        assertEquals(NAMES.get(index % NAMES.size()), reader.readString());
      }
    }
  }
}
