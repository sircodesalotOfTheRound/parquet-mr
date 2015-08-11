package org.apache.parquet.parqour.ffreaders;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.ffreader.plain.PlainSingleFastForwardReader;
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
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/13/15.
 */
public class TestSingleFFReader extends UsesPersistence {
  private static int TOTAL = TestTools.generateRandomInt(1000);
  private static String COLUMN_NAME = "double";
  private static Iterable<Float> NUMBERS = generateNumbers();

  private static List<Float> generateNumbers() {
    List<Float> numbers = new ArrayList<Float>();
    Random random = new Random(System.currentTimeMillis());
    for (int index = 0; index < TOTAL; index++) {
      numbers.add(random.nextFloat());
    }

    return numbers;
  }

  public static class SingleDoubleWriteContext extends WriteTools.ParquetWriteContext {
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "count",
      new PrimitiveType(REQUIRED, FLOAT, COLUMN_NAME));

    public SingleDoubleWriteContext(ParquetConfiguration configuration) {
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
  public void testPlainSingleFFReader() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.generateTestData(new SingleDoubleWriteContext(configuration));

      QueryInfo queryInfo = TestTools.generateSchemaInfoFromPath(TestTools.TEST_FILE_PATH);
      DiskInterfaceManager_OLD diskInterfaceManager = new DiskInterfaceManager_OLD(queryInfo);
      ColumnDescriptor squaredColumn = queryInfo.getColumnDescriptorByPath(COLUMN_NAME);
      DataPageDecorator page = diskInterfaceManager.getFirstPageForColumn(squaredColumn);
      PlainSingleFastForwardReader reader = page.valuesReader();

      for (float number : NUMBERS) {
        assertEquals(number, reader.readSingle());
      }
    }
  }
}
