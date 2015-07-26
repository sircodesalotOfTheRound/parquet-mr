package org.apache.parquet.parqour.testtools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.RelationshipLevelFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageDecorator;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.*;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by sircodesalot on 6/8/15.
 */
public class TestTools {
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  public static final int ONE_MB = (int) Math.pow(2, 20);
  public static final ParquetProperties.WriterVersion[] PARQUET_VERSIONS = {PARQUET_1_0 , PARQUET_2_0 };
  public static final String TEST_FILE_PATH = "test_output.parq";
  public static final String EMPTY_STRING = "";
  public static final String CURRENT_DIRECTORY = ".";
  public static final ParquetConfiguration[] CONFIGURATIONS = ParquetConfiguration.values();
  public static final MessageType SINGLE_COLUMN_SCHEMA = new MessageType("single_column_schema",
    new PrimitiveType(REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "numeric_column"));

  public static final MessageType CONTACTS_SCHEMA =
    new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "owner"),
      new PrimitiveType(REPEATED, BINARY, "ownerPhoneNumbers"),
      new GroupType(REPEATED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "name"),
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

  public static final Configuration EMPTY_CONFIGURATION = new Configuration();
  public static final Operators.IntColumn NUMERIC_COLUMN = intColumn("numeric_column");
  public static final Operators.BinaryColumn BINARY_COLUMN = binaryColumn("binary_column");

  public static class FirstNames implements Iterable<String> {
    public List<String> FIRST_NAMES = new ArrayList<String>() {{
      add("sammy");
      add("louis");
      add("samantha");
      add("rob");
      add("juliange");
      add("siddartha");
      add("jose");
      add("rebecca");
      add("thomas");
      add("jean patrick");
    }};

    public String getModulo(int index) { return FIRST_NAMES.get(index % FIRST_NAMES.size()); }

    @Override
    public Iterator<String> iterator() {
      return FIRST_NAMES.iterator();
    }
  }

  public static FirstNames FIRST_NAMES = new FirstNames();

  public static ColumnDescriptor getColumnFromSchema(MessageType schema, String... path) {
    return schema.getColumnDescription(path);
  }

  public static SchemaInfo generateSchemaInfoFromSchema(GroupType schema) {
    return new SchemaInfo(new Configuration(), new Path(CURRENT_DIRECTORY), generateEmptyMetadataFromSchema(schema), schema);
  }

  public static SchemaInfo generateSchemaInfoFromSchema(GroupType schema, FilterPredicate predicate) {
    return new SchemaInfo(new Configuration(), new Path(CURRENT_DIRECTORY), generateEmptyMetadataFromSchema(schema), schema, predicate);
  }

  public static IngestTree generateIngestTreeFromSchema(GroupType schema) {
    SchemaInfo schemaInfo = generateSchemaInfoFromSchema(schema);
    return new IngestTree(schemaInfo, mockDiskInterfaceManager());
  }

  public static IngestTree generateIngestTreeFromSchema(GroupType schema, FilterPredicate predicate) {
    SchemaInfo schemaInfo = generateSchemaInfoFromSchema(schema, predicate);
    return new IngestTree(schemaInfo, mockDiskInterfaceManager());
  }

  public static ParquetMetadata generateEmptyMetadataFromSchema(GroupType schema) {
    MessageType messageSchema = new MessageType(schema.getName(), schema.getFields());
    FileMetaData fileMetaData = new FileMetaData(messageSchema, new HashMap<String, String>(), EMPTY_STRING);
    return new ParquetMetadata(fileMetaData, new ArrayList<BlockMetaData>());
  }

  public static SchemaInfo generateSchemaInfoFromPath(String path) throws Exception {
    Path filePath = new Path(path);
    ParquetMetadata metadata = ParquetFileReader.readFooter(EMPTY_CONFIGURATION,
      filePath,
      ParquetMetadataConverter.NO_FILTER);

    MessageType messageSchema = metadata.getFileMetaData().getSchema();

    return new SchemaInfo(EMPTY_CONFIGURATION, filePath, metadata, messageSchema);
  }

  public static void generateTestData(WriteTools.ParquetWriteContext context) {
    WriteTools.withParquetWriter(context);
  }

  private static DiskInterfaceManager mockDiskInterfaceManager() {
    DiskInterfaceManager diskInterfaceManager = mock(DiskInterfaceManager.class);
    DataPageDecorator dataPageDecorator = mock(DataPageDecorator.class);

    RelationshipLevelFastForwardReader mockDlRlReader = mockDlRlReader();
    when (dataPageDecorator.definitionLevelReader()).thenReturn(mockDlRlReader);
    when (dataPageDecorator.repetitionLevelReader()).thenReturn(mockDlRlReader);

    when(diskInterfaceManager.getFirstPageForColumn(any(ColumnDescriptor.class))).thenReturn(dataPageDecorator);
    when(dataPageDecorator.totalItems()).thenReturn((long)0);

    return diskInterfaceManager;
  }

  private static RelationshipLevelFastForwardReader mockDlRlReader() {
    RelationshipLevelFastForwardReader reader = mock(RelationshipLevelFastForwardReader.class);
    when(reader.isEof()).thenReturn(true);

    return reader;
  }

  public interface RepeatCallback { void execute() throws Exception; }
  public static void repeat(int times, RepeatCallback callback) throws Exception {
    for (int index = 0; index < times; index++) {
      double startTime = System.currentTimeMillis();
      callback.execute();

      String timeMessage = String.format("Time: %s", ((double)System.currentTimeMillis() - startTime) / 1000.0);
      System.err.println(timeMessage);
    }
  }

  public static int generateRandomInt(int max) {
    return RANDOM.nextInt(max);
  }

  public static void deleteTestData() {
    WriteTools.deleteTestData();
  }
}
