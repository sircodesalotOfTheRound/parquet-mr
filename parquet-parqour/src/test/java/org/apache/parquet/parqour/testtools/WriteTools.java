package org.apache.parquet.parqour.testtools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;

import static org.apache.parquet.parqour.testtools.TestTools.ONE_MB;
import static org.apache.parquet.parqour.testtools.TestTools.TEST_FILE_PATH;

/**
* Created by sircodesalot on 6/11/15.
*/

public class WriteTools {
  public static abstract class ParquetWriteContext {
    private final MessageType schema;
    private final int blockSizeInMB;
    private final int pageSizeInMB;
    private final boolean enableDictionary;
    private final ParquetProperties.WriterVersion version;

    public ParquetWriteContext(GroupType schema, ParquetProperties.WriterVersion version, int blockSizeInMB, int pageSizeInMB, boolean enableDictionary) {
      this.schema = new MessageType(schema.getName(), schema.getFields());
      this.version = version;
      this.blockSizeInMB = blockSizeInMB;
      this.pageSizeInMB = pageSizeInMB;
      this.enableDictionary = enableDictionary;
    }

    public abstract void write(ParquetWriter<Group> writer) throws IOException;

    public MessageType schema() {
      return this.schema;
    }

    public int blockSizeInMB() {
      return this.blockSizeInMB;
    }

    public int pageSizeInMB() {
      return this.pageSizeInMB;
    }

    public boolean enableDictionary() { return this.enableDictionary; }

    public ParquetProperties.WriterVersion version() {
      return this.version;
    }
  }

  public static abstract class TestableParquetWriteContext extends ParquetWriteContext {
    public TestableParquetWriteContext(GroupType schema, ParquetConfiguration configuration) {
      super(schema, configuration.version(), 1, 1, configuration.useDictionary());
    }

    public abstract void test();
  }

  public static void withParquetWriter(ParquetWriteContext context) {
    deleteTestData();

    Path path = new Path(TEST_FILE_PATH);

    // Create the configuration, and then apply the schema to our configuration.
    Configuration configuration = new Configuration();
    GroupWriteSupport.setSchema(context.schema(), configuration);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();

    // Create the writer properties
    final int blockSize = context.blockSizeInMB() * ONE_MB;
    final int pageSize = context.pageSizeInMB() * ONE_MB;
    final int dictionaryPageSize = ONE_MB / 2;
    final boolean enableDictionary = context.enableDictionary();
    final boolean enableValidation = false;
    ParquetProperties.WriterVersion writerVersion = context.version();
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

    try (ParquetWriter<Group> writer = new ParquetWriter<Group>(path, groupWriteSupport, codec, blockSize,
      pageSize, dictionaryPageSize, enableDictionary, enableValidation, writerVersion, configuration)) {

      // Use the callback to perform the write.
      context.write(writer);
    } catch (IOException ex) {
      throw new DataIngestException("IO Exception");
    }
  }

  public static <T extends TestableParquetWriteContext> void generateDataAndTest(int times, Class<T> forType) throws Exception {
    for (final ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      final TestableParquetWriteContext context = (TestableParquetWriteContext)forType.getConstructors()[0].newInstance(configuration);
      withParquetWriter(context);

      TestTools.repeat(times, new TestTools.RepeatCallback() {
        @Override
        public void execute() throws Exception {
          context.test();
        }
      });
    }
  }


  public static void deleteTestData() {
    File file = new File(TEST_FILE_PATH);

    // Delete if it exists.
    if (file.exists()) {
      file.delete();
    }
  }
}
