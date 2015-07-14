package org.apache.parquet.parqour.ingest.tree;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/10/15.
 */
public class BrokenTest {
  private static final String TEST_FILE_PATH = "sometest.parq";
  private static final int TOTAL_ROWS = new Random().nextInt(1000);
  private static final int FIELD_NUMBER_TWO = 1;
  public static final int ONE_MB = (int) Math.pow(2, 20);

  private final MessageType COMPLEX_SCHEMA = new MessageType("schema",
    new PrimitiveType(REQUIRED, INT32, "one"),
    new PrimitiveType(OPTIONAL, INT32, "two"),
    new PrimitiveType(REPEATED, INT32, "three"),
    new GroupType(OPTIONAL, "fizz_buzz",
      new PrimitiveType(OPTIONAL, INT32, "fizz"),
      new PrimitiveType(OPTIONAL, INT32, "buzz")),
    new GroupType(OPTIONAL, "first-group",
      new PrimitiveType(REPEATED, INT32, "first-0"),
      new PrimitiveType(REPEATED, INT32, "first-1"),
      new PrimitiveType(REPEATED, INT32, "first-2"),
      new GroupType(OPTIONAL, "second-group",
        new PrimitiveType(REPEATED, INT32, "second-0"),
        new PrimitiveType(REPEATED, INT32, "second-1"),
        new PrimitiveType(REPEATED, INT32, "second-2"),
        new PrimitiveType(REPEATED, INT32, "second-3"),
        new PrimitiveType(REPEATED, INT32, "second-4"),
        new GroupType(OPTIONAL, "third-group",
          new PrimitiveType(REPEATED, INT32, "third-0"),
          new PrimitiveType(REPEATED, INT32, "third-1"),
          new PrimitiveType(REPEATED, INT32, "third-2"),
          new PrimitiveType(REPEATED, INT32, "third-3"),
          new PrimitiveType(REPEATED, INT32, "third-4"),
          new PrimitiveType(REPEATED, INT32, "third-5")))),
    new GroupType(REPEATED, "repeatA",
      new PrimitiveType(OPTIONAL, INT32, "repeatA-0"),
      new PrimitiveType(REPEATED, INT32, "repeatA-1"),
      new PrimitiveType(REPEATED, INT32, "repeatA-2"),
      new GroupType(REPEATED, "repeatB",
        new PrimitiveType(REPEATED, INT32, "repeatB-0"),
        new PrimitiveType(REPEATED, INT32, "repeatB-1"),
        new PrimitiveType(REPEATED, INT32, "repeatB-2"),
        new PrimitiveType(REPEATED, INT32, "repeatB-3"),
        new PrimitiveType(REPEATED, INT32, "repeatB-4"),
        new GroupType(REPEATED, "repeatC",
          new PrimitiveType(REPEATED, INT32, "repeatC-0"),
          new PrimitiveType(REPEATED, INT32, "repeatC-1"),
          new PrimitiveType(REPEATED, INT32, "repeatC-2"),
          new PrimitiveType(REPEATED, INT32, "repeatC-3"),
          new PrimitiveType(REPEATED, INT32, "repeatC-4"),
          new PrimitiveType(REPEATED, INT32, "repeatC-5")))),
    new PrimitiveType(OPTIONAL, INT32, "last"));

  public void writeData(WriteCallback<Group> callback) throws Exception {
    File file = new File(TEST_FILE_PATH);
    if (file.exists()) {
      file.delete();
    }

    Path path = new Path(TEST_FILE_PATH);

    // Create the configuration, and then apply the schema to our configuration.
    Configuration configuration = new Configuration();
    GroupWriteSupport.setSchema(COMPLEX_SCHEMA, configuration);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();

    // Create the writer properties
    final int blockSize =  1 * ONE_MB;
    final int pageSize = 1 * ONE_MB;
    final int dictionaryPageSize = ONE_MB / 2;
    final boolean enableDictionary = false;
    final boolean enableValidation = false;
    ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion.PARQUET_1_0;
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

    try (ParquetWriter<Group> writer = new ParquetWriter<Group>(path, groupWriteSupport, codec, blockSize,
      pageSize, dictionaryPageSize, enableDictionary, enableValidation, writerVersion, configuration)) {

      callback.write(writer);
      // Use the callback to perform the write.
    } catch (IOException ex) {
      throw new Exception("IO Exception");
    }
  }

  interface WriteCallback<T> {
    void write(ParquetWriter<T> writer) throws Exception;
  };

  public void doPrepWork() throws Exception {
    writeData(new WriteCallback<Group>() {
      @Override
      public void write(ParquetWriter<Group> writer) throws Exception {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group schema = new SimpleGroup(COMPLEX_SCHEMA);

          // (1) Always add and increment.
          schema.append("one", index);

          // (2) Only add when even.
          if (index % 2 == 0) {
            schema.append("two", index * 2);
          }
          // (3) Create a simple list.
          for (int repeat = 0; repeat < (index % 5) + 1; repeat++) {
            schema.append("three", (index * 3) + repeat);
          }

          // (4) Fizz buzz tree.sudo reboot

          if (index % 3 == 0 || index % 5 == 0) {
            Group optionalGroup = schema.addGroup("fizz_buzz");
            if (index % 3 == 0) {
              optionalGroup.add("fizz", index);
            }

            if (index % 5 == 0) {
              optionalGroup.add("buzz", index);
            }
          }

          // (5) Variable levels and schemas
          if (index % 2 == 0) {
            Group first = schema.addGroup("first-group");
            repeatAdd(first, "first-0", index % 3, index);
            repeatAdd(first, "first-1", index % 5, index);
            repeatAdd(first, "first-2", index % 7, index);

            if (index % 3 == 0) {
              Group second = first.addGroup("second-group");
              repeatAdd(second, "second-0", index % 4, index);
              repeatAdd(second, "second-1", index % 5, index);
              repeatAdd(second, "second-2", index % 6, index);
              repeatAdd(second, "second-3", index % 7, index);
              repeatAdd(second, "second-4", index % 8, index);

              if (index % 4 == 0) {
                Group third = second.addGroup("third-group");
                repeatAdd(third, "third-0", index % 2, index);
                repeatAdd(third, "third-1", index % 3, index);
                repeatAdd(third, "third-2", index % 4, index);
                repeatAdd(third, "third-3", index % 5, index);
                repeatAdd(third, "third-4", index % 6, index);
                repeatAdd(third, "third-5", index % 7, index);
              }
            }
          }

          // (6) Variable Repeats
          for (int repeatAIndex = 0; repeatAIndex < index % 5; repeatAIndex++) {
            Group repeatA = schema.addGroup("repeatA");
            repeatAdd(repeatA, "repeatA-0", index % 2, index);
            repeatAdd(repeatA, "repeatA-1", index % 3, index);
            repeatAdd(repeatA, "repeatA-2", index % 4, index);

            for (int repeatBIndex = 0; repeatBIndex < index % 3; repeatBIndex++) {
              Group repeatB = repeatA.addGroup("repeatB");
              repeatAdd(repeatB, "repeatB-0", index % 4, index);
              repeatAdd(repeatB, "repeatB-1", index % 3, index);
              repeatAdd(repeatB, "repeatB-2", index % 2, index);
              repeatAdd(repeatB, "repeatB-3", index % 3, index);
              repeatAdd(repeatB, "repeatB-4", index % 4, index);

              for (int repeatCIndex = 0; repeatCIndex < index % 2; repeatCIndex++) {
                Group repeatC = repeatB.addGroup("repeatC");
                repeatAdd(repeatC, "repeatC-0", index % 3, index);
                repeatAdd(repeatC, "repeatC-1", index % 5, index);
                repeatAdd(repeatC, "repeatC-2", index % 2, index);
                repeatAdd(repeatC, "repeatC-3", index % 5, index);
                repeatAdd(repeatC, "repeatC-4", index % 3, index);
              }
            }
          }

          writer.write(schema);
        }
      }
    });

  }

  private void repeatAdd(Group group, String field, int times, int startValue) {
    for (int index = 0; index < times; index++) {
      group.append(field, index + startValue);
    }
  }

  public void theActualTest() throws Exception {
    this.doPrepWork();

    /////////////////////////// THE ACTUAL TEST ////////////////////////
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(TEST_FILE_PATH)).withConf(new Configuration())
      .withFilter(FilterCompat.get(FilterApi.eq(FilterApi.intColumn("repeatA.repeatA-0"), 1)))
      .build();

    for (int index = 0; index < TOTAL_ROWS; index++) {
      Group schema = reader.read();

      for (int repeatAIndex = 0; repeatAIndex < schema.getFieldRepetitionCount("repeatA"); repeatAIndex++) {
        Group repeatA = schema.getGroup("repeatA", repeatAIndex);
        repeatCheck(repeatA, "repeatA-0", index % 2, index);
        repeatCheck(repeatA, "repeatA-1", index % 3, index);
        repeatCheck(repeatA, "repeatA-2", index % 4, index);

        for (int repeatBIndex = 0; repeatBIndex < repeatA.getFieldRepetitionCount("repeatB"); repeatBIndex++) {
          Group repeatB = repeatA.getGroup("repeatB", repeatBIndex);
          repeatCheck(repeatB, "repeatB-0", index % 4, index);
          repeatCheck(repeatB, "repeatB-1", index % 3, index);
          repeatCheck(repeatB, "repeatB-2", index % 2, index);
          repeatCheck(repeatB, "repeatB-3", index % 3, index);
          repeatCheck(repeatB, "repeatB-4", index % 4, index);

          for (int repeatCIndex = 0; repeatCIndex < repeatB.getFieldRepetitionCount("repeatC"); repeatCIndex++) {
            Group repeatC = repeatB.getGroup("repeatC", repeatCIndex);
            repeatCheck(repeatC, "repeatC-0", index % 3, index);
            repeatCheck(repeatC, "repeatC-1", index % 5, index);
            repeatCheck(repeatC, "repeatC-2", index % 2, index);
            repeatCheck(repeatC, "repeatC-3", index % 5, index);
            repeatCheck(repeatC, "repeatC-4", index % 3, index);
          }
        }
      }
    }
  }

  public void repeatCheck(Group group, String field, int times, int startValue) {
    int count = 0;
    for (int index = 0; index < group.getFieldRepetitionCount(field); index++) {
      assertEquals(group.getInteger(field, index), index + startValue);
      count++;
    }

    assertEquals(count, times);
  }
}