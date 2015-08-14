package org.apache.parquet.parqour.ffreaders;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.manager.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.disk.pages.Page;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int64FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.RelationshipLevelFastForwardReader;
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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/13/15.
 */
public class TestRelationshipLevelFastForwardReaders extends UsesPersistence {
  private static int TOTAL = TestTools.generateRandomInt(40000);
  private static String NODE = "node";
  private static String LEAF = "node.node.node.node.node";

  public static class VaryingDefinitionLevelWriteContext extends WriteTools.ParquetWriteContext {
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "count",
      new GroupType(OPTIONAL, NODE,
        new GroupType(OPTIONAL, NODE,
          new GroupType(REPEATED, NODE,
            new GroupType(OPTIONAL, NODE,
              new PrimitiveType(OPTIONAL, INT32, NODE))))));

    public VaryingDefinitionLevelWriteContext(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL; index++) {
        SimpleGroup root = new SimpleGroup(SCHEMA);
        Group column = root;

        for (int nodeIndex = 0; nodeIndex <= index % 5; nodeIndex++) {
          if (nodeIndex == 4) {
            column.append(NODE, 1);
          } else {
            column = column.addGroup(NODE);
          }
        }

        writer.write(root);
      }
    }
  }

  @Test
  public void testVariableDefinitionLevels() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIGURATION %s: TOTAL: %s", configuration, TOTAL);
      TestTools.generateTestData(new VaryingDefinitionLevelWriteContext(configuration));

      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(LEAF).iterator().next();

      RelationshipLevelFastForwardReader repetitionLevelReader = page.repetitionLevelReader();
      RelationshipLevelFastForwardReader definitionLevelReader = page.definitionLevelReader();

      for (int index = 0; index < TOTAL; index++) {
        int definitionLevel = (index % 5) + 1;
        int definitionLevelFromReader = definitionLevelReader.nextRelationshipLevel();
        int repetitionLevelFromReader = repetitionLevelReader.nextRelationshipLevel();

        assertEquals(definitionLevel, definitionLevelFromReader);
        assertEquals(0, repetitionLevelFromReader);
      }
    }
  }

  public static class SingleDefinitionWriteContext extends WriteTools.ParquetWriteContext {
    public static final String COLUMN_NAME = "int_column";
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "count",
      new PrimitiveType(OPTIONAL, INT32, COLUMN_NAME));

    public SingleDefinitionWriteContext(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL; index++) {
        Group column = new SimpleGroup(SCHEMA).append(COLUMN_NAME, 1);
        writer.write(column);
      }
    }
  }

  @Test
  public void testSingleDefinitionLevelColumn() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIGURATION %s: TOTAL: %s", configuration, TOTAL);
      TestTools.generateTestData(new SingleDefinitionWriteContext(configuration));

      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(SingleDefinitionWriteContext.COLUMN_NAME).iterator().next();

      RelationshipLevelFastForwardReader definitionLevelReader = page.definitionLevelReader();

      for (int index = 0; index < TOTAL; index++) {
        assertEquals(1, definitionLevelReader.nextRelationshipLevel());
      }
    }
  }

  public static class VariableRepetitionLevelWriteContext32 extends WriteTools.ParquetWriteContext {
    public static final String COLUMN_NAME = "repeat";
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "repeat",
      new PrimitiveType(REPEATED, INT32, COLUMN_NAME));

    public VariableRepetitionLevelWriteContext32(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL; index++) {
        Group column = new SimpleGroup(SCHEMA);
        for (int repeatCount = 0; repeatCount < (index % 7) + 1; repeatCount++) {
          column.append("repeat", index % 7);
        }

        writer.write(column);
      }
    }
  }

  @Test
  public void testVariableRepetitionLevelsInt32() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIGURATION %s: TOTAL: %s", configuration, TOTAL);
      TestTools.generateTestData(new VariableRepetitionLevelWriteContext32(configuration));

      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(VariableRepetitionLevelWriteContext32.COLUMN_NAME).iterator().next();

      RelationshipLevelFastForwardReader repetitionLevelReader = page.repetitionLevelReader();
      RelationshipLevelFastForwardReader definitionLevelReader = page.definitionLevelReader();
      Int32FastForwardReader valuesReader = page.contentReader();

      int totalRead = 0;
      int index = 0;
      while (totalRead < TOTAL) {
        int definitionLevelFromReader = definitionLevelReader.nextRelationshipLevel();
        int repetitionLevelFromReader = repetitionLevelReader.nextRelationshipLevel();
        int value = valuesReader.readi32();

        assertEquals(1, definitionLevelFromReader);
        assertEquals(0, repetitionLevelFromReader);
        assertEquals(index % 7, value);

        totalRead++;

        for (int repeatCount = 0; repeatCount < (index % 7) && totalRead < TOTAL; repeatCount++) {
          definitionLevelFromReader = definitionLevelReader.nextRelationshipLevel();
          repetitionLevelFromReader = repetitionLevelReader.nextRelationshipLevel();
          value = valuesReader.readi32();

          assertEquals(1, definitionLevelFromReader);
          assertEquals(1, repetitionLevelFromReader);
          assertEquals(index % 7, value);

          totalRead++;
        }

        index++;
      }
    }
  }


  public static class VariableRepetitionLevelWriteContext64 extends WriteTools.ParquetWriteContext {
    public static final String COLUMN_NAME = "repeat";
    private static final GroupType SCHEMA = new GroupType(REQUIRED, "repeat",
      new PrimitiveType(REPEATED, INT64, COLUMN_NAME));

    public VariableRepetitionLevelWriteContext64(ParquetConfiguration configuration) {
      super(SCHEMA, configuration.version(), 1, 1, configuration.useDictionary());
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL; index++) {
        Group column = new SimpleGroup(SCHEMA);
        for (int repeatCount = 0; repeatCount < (index % 7) + 1; repeatCount++) {
          column.append("repeat", (long)index % 7);
        }

        writer.write(column);
      }
    }
  }

  @Test
  public void testVariableRepetitionLevelsInt64() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      TestTools.printerr("CONFIGURATION %s: TOTAL: %s", configuration, TOTAL);
      TestTools.generateTestData(new VariableRepetitionLevelWriteContext64(configuration));


      HDFSParquetFile file = new HDFSParquetFile(TestTools.EMPTY_CONFIGURATION, TestTools.TEST_FILE_PATH);
      HDFSParquetFileMetadata metadata = new HDFSParquetFileMetadata(file);
      DiskInterfaceManager diskInterfaceManager = new DiskInterfaceManager(metadata);
      Page page = diskInterfaceManager.pagerFor(VariableRepetitionLevelWriteContext64.COLUMN_NAME).iterator().next();
      RelationshipLevelFastForwardReader definitionLevelReader = page.definitionLevelReader();
      RelationshipLevelFastForwardReader repetitionLevelReader = page.repetitionLevelReader();
      Int64FastForwardReader valuesReader = page.contentReader();

      int totalRead = 0;
      int index = 0;
      while (totalRead < TOTAL) {

        int definitionLevelFromReader = definitionLevelReader.nextRelationshipLevel();
        int repetitionLevelFromReader = repetitionLevelReader.nextRelationshipLevel();
        long value = valuesReader.readi64();

        assertEquals(1, definitionLevelFromReader);
        assertEquals(0, repetitionLevelFromReader);
        assertEquals(index % 7, value);

        totalRead++;

        for (int repeatCount = 0; repeatCount < (index % 7) && totalRead < TOTAL; repeatCount++) {
          definitionLevelFromReader = definitionLevelReader.nextRelationshipLevel();
          repetitionLevelFromReader = repetitionLevelReader.nextRelationshipLevel();
          value = valuesReader.readi64();

          assertEquals(1, definitionLevelFromReader);
          assertEquals(1, repetitionLevelFromReader);
          assertEquals(index % 7, value);

          totalRead++;
        }

        index++;
      }
    }
  }


}
