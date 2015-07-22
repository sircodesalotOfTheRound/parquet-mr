package org.apache.parquet.parqour.ingest.driver;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by sircodesalot on 6/22/15.
 */
public class TestVariableSchemas {
  private static final int TOTAL_ROWS = TestTools.generateRandomInt(100000);

  public static class SingleRequiredColumnSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("single_column",
      new PrimitiveType(REQUIRED, INT32, "single_required_column"));

    public SingleRequiredColumnSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS; index++) {
        Group instance = new SimpleGroup(SCHEMA);
        instance.append("single_required_column", index);

        writer.write(instance);
      }
    }

    public void test() {
      Integer index = 0;
      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        assertEquals(index, cursor.i32("single_required_column"));
        index++;
      }
    }
  }

  public static class SingleOptionalColumnSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("single_column",
      new PrimitiveType(OPTIONAL, INT32, "optional_column"));

    public SingleOptionalColumnSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS; index++) {
        Group instance = new SimpleGroup(SCHEMA);

        if (index % 2 == 0) {
          instance.append("optional_column", index * 2);
        }

        writer.write(instance);
      }
    }

    public void test() {
      Integer index = 0;
      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        if (index % 2 == 0) {
          assertEquals((Integer) (index * 2), cursor.i32("optional_column"));
        } else {
          assertNull(cursor.i32("optional_column"));
        }

        index++;
      }
    }
  }

  public static class MultiplyNestedOptionalSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("single_column",
      new GroupType(OPTIONAL, "first",
        new GroupType(OPTIONAL, "second",
          new GroupType(OPTIONAL, "third",
            new GroupType(OPTIONAL, "fourth",
              new PrimitiveType(OPTIONAL, INT32, "fifth"))))));

    public MultiplyNestedOptionalSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS; index++) {
        Group instance = new SimpleGroup(SCHEMA);

        if (index % 2 == 0) {
          Group first = instance.addGroup("first");
          if (index % 3 == 0) {
            Group second = first.addGroup("second");
            if (index % 4 == 0) {
              Group third = second.addGroup("third");
              if (index % 5 == 0) {
                Group fourth = third.addGroup("fourth");
                if (index % 6 == 0) {
                  fourth.append("fifth", index);
                }
              }
            }
          }
        }

        writer.write(instance);
      }
    }

    public void test() {
      int index = 0;
      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        if (index % 2 == 0) {
          Cursor first = cursor.field("first");
          if (index % 3 == 0) {
            Cursor second = first.field("second");
            if (index % 4 == 0) {
              Cursor third = second.field("third");
              if (index % 5 == 0) {
                Cursor fourth = third.field("fourth");
                if (index % 6 == 0) {
                  assertEquals((Integer) index, fourth.i32("fifth"));
                } else {
                  assertNull(fourth.i32("fifth"));
                }
              } else {
                assertNull(third.field("fourth"));
              }
            } else {
              assertNull(second.field("third"));
            }
          } else {
            assertNull(first.field("second"));
          }
        } else {
          assertNull(cursor.field("first"));
        }

        index++;
      }
    }
  }


  public static class FizzBuzzSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("single_column",
      new GroupType(OPTIONAL, "fizz_buzz",
        new PrimitiveType(OPTIONAL, INT32, "fizz"),
        new PrimitiveType(OPTIONAL, INT32, "buzz")));

    public FizzBuzzSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS; index++) {
        Group instance = new SimpleGroup(SCHEMA);

        if (index % 3 == 0 || index % 5 == 0) {
          Group optionalGroup = instance.addGroup("fizz_buzz");
          if (index % 3 == 0) {
            optionalGroup.add("fizz", index);
          }

          if (index % 5 == 0) {
            optionalGroup.add("buzz", index);
          }
        }

        writer.write(instance);
      }
    }

    public void test() {
      Integer index = 0;

      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        if (index % 3 == 0 || index % 5 == 0) {
          if (index % 3 == 0) {
            assertEquals(index, cursor.field("fizz_buzz").i32("fizz"));
          } else {
            assertNull(cursor.field("fizz_buzz").field("fizz"));
          }

          if (index % 5 == 0) {
            assertEquals(index, cursor.field("fizz_buzz").i32("buzz"));
          } else {
            assertNull(cursor.field("fizz_buzz").field("buzz"));
          }
        } else {
          assertNull(cursor.field("fizz_buzz"));
        }

        index++;
      }
    }
  }

  public static class SingleRepeatColumnSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("single_column",
      new PrimitiveType(REPEATED, INT32, "repeat"));

    public SingleRepeatColumnSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS; index++) {
        Group instance = new SimpleGroup(SCHEMA);

        for (int repeat = 0; repeat < (index % 5); repeat++) {
          instance.append("repeat", (index * 3) + repeat);
        }

        writer.write(instance);
      }
    }

    public void test() {
      Integer index = 0;

      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        int repeat = 0;
        for (int value : cursor.i32Iter("repeat")) {
          assertEquals((index * 3) + repeat, value);
          repeat++;
        }
        assertEquals((index % 5), repeat);
        index++;
      }
    }
  }

  public static class GroupRepetitionSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("group_repeat",
      new GroupType(REPEATED, "group-list",
        new PrimitiveType(REQUIRED, INT32, "value")));

    public GroupRepetitionSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS; index++) {
        Group instance = new SimpleGroup(SCHEMA);

        for (int repeat = 0; repeat < (index % 5); repeat++) {
          Group group = instance.addGroup("group-list");
          group.add("value", index + repeat);
        }

        writer.write(instance);
      }
    }

    public void test() {
      Integer index = 0;

      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        int repeat = 0;
        for (Cursor group : cursor.fieldIter("group-list")) {
          assertEquals(index + repeat, (int) group.i32("value"));
          repeat++;
        }
        assertEquals((index % 5), repeat);
        index++;
      }
    }
  }

  public static class MultipleRepeatingColumnPrimitiveColumnSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("group_repeat",
      new PrimitiveType(REPEATED, INT32, "first"),
      new PrimitiveType(REPEATED, INT32, "second"),
      new PrimitiveType(REPEATED, INT32, "third"));

    public MultipleRepeatingColumnPrimitiveColumnSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS; index++) {
        Group instance = new SimpleGroup(SCHEMA);

        for (int firstRepeat = 0; firstRepeat < (index % 2); firstRepeat++) {
          instance.add("first", index + firstRepeat);
        }

        for (int secondRepeat = 0; secondRepeat < (index % 3); secondRepeat++) {
          instance.add("second", index + secondRepeat);
        }

        for (int thirdRepeat = 0; thirdRepeat < (index % 5); thirdRepeat++) {
          instance.add("third", index + thirdRepeat);
        }

        writer.write(instance);
      }
    }

    public void test() {
      Integer index = 0;

      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        int repeat = 0;
        for (Integer value : cursor.i32Iter("first")) {
          assertEquals(index + repeat, (int) value);
          repeat++;
        }
        assertEquals((index % 2), repeat);

        repeat = 0;
        for (Integer value : cursor.i32Iter("second")) {
          assertEquals(index + repeat, (int) value);
          repeat++;
        }
        assertEquals((index % 3), repeat);

        repeat = 0;
        for (Integer value : cursor.i32Iter("third")) {
          assertEquals(index + repeat, (int) value);
          repeat++;
        }
        assertEquals((index % 5), repeat);

        index++;
      }
    }
  }

  public static class LongListSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("group_repeat",
      new PrimitiveType(REPEATED, INT32, "value"));

    public LongListSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS % 100; index++) {
        Group instance = new SimpleGroup(SCHEMA);

        for (int repeat = 0; repeat < 1000; repeat++) {
          instance.append("value", index + repeat);
        }

        writer.write(instance);
      }
    }

    public void test() {
      Integer index = 0;

      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        int repeat = 0;
        for (int value : cursor.i32Iter("value")) {
          assertEquals(index + repeat, value);
          repeat++;
        }
        index++;
      }
    }
  }

  public static class SingleNestedRepeatSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("group_repeat",
      new GroupType(REPEATED, "first_repeat",
        new GroupType(REPEATED, "second_repeat",
          new PrimitiveType(REPEATED, INT32, "third_repeat"))));

    public SingleNestedRepeatSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS; index++) {
        Group instance = new SimpleGroup(SCHEMA);

        for (int firstRepeat = 0; firstRepeat < (index % 3); firstRepeat++) {
          Group first = instance.addGroup("first_repeat");
          for (int secondRepeat = 0; secondRepeat < (index % 4); secondRepeat++) {
            Group second = first.addGroup("second_repeat");
            for (int thirdRepeat = 0; thirdRepeat < (index % 5); thirdRepeat++) {
              second.append("third_repeat", index + secondRepeat + thirdRepeat);
            }
          }
        }
        writer.write(instance);
      }
    }

    public void test() {
      Integer index = 0;

      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        int firstRepeat = 0;
        for (Cursor first : cursor.fieldIter("first_repeat")) {
          int secondRepeat = 0;
          for (Cursor second : first.fieldIter("second_repeat")) {
            int thirdRepeat = 0;
            for (int value : second.i32Iter("third_repeat")) {
              assertEquals(index + secondRepeat + thirdRepeat, value);
              thirdRepeat++;
            }
            assertEquals(index % 5, thirdRepeat);
            secondRepeat++;
          }
          assertEquals(index % 4, secondRepeat);
          firstRepeat++;
        }
        assertEquals(index % 3, firstRepeat);
        index++;
      }
    }
  }

  public static class MultiplyNestedMultiLevelSchema extends WriteTools.TestableParquetWriteContext {
    private static MessageType SCHEMA = new MessageType("group_repeat",
      new GroupType(REPEATED, "repeatA",
        new PrimitiveType(REPEATED, INT32, "repeatA-0"),
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
            new PrimitiveType(REPEATED, INT32, "repeatC-5")))));

    public MultiplyNestedMultiLevelSchema(ParquetConfiguration configuration) {
      super(SCHEMA, configuration);
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < TOTAL_ROWS; index++) {
        Group instance = new SimpleGroup(SCHEMA);

        for (int repeatAIndex = 0; repeatAIndex < index % 5; repeatAIndex++) {
          Group repeatA = instance.addGroup("repeatA");
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
              repeatAdd(repeatC, "repeatC-0", index % 2, index);
              repeatAdd(repeatC, "repeatC-1", index % 3, index);
              repeatAdd(repeatC, "repeatC-2", index % 4, index);
              repeatAdd(repeatC, "repeatC-3", index % 5, index);
              repeatAdd(repeatC, "repeatC-4", index % 6, index);
              repeatAdd(repeatC, "repeatC-5", index % 7, index);
            }
          }
        }

        writer.write(instance);
      }
    }


    private void repeatAdd(Group group, String field, int times, int startValue) {
      for (int index = 0; index < times; index++) {
        group.append(field, index + startValue);
      }
    }

    private void repeatCheck(Iterable<Integer> items, int times, int startValue) {
      int count = 0;
      for (int item : items) {
        assertEquals(item, startValue++);
        count++;
      }

      assertEquals(count, times);
    }

    @Override
    public void test() {
      int index = 0;
      for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
        int repeatACount = 0;
        for (Cursor repeatA : cursor.fieldIter("repeatA")) {
          repeatCheck(repeatA.i32Iter("repeatA-0"), index % 2, index);
          repeatCheck(repeatA.i32Iter("repeatA-1"), index % 3, index);
          repeatCheck(repeatA.i32Iter("repeatA-2"), index % 4, index);

          int repeatBCount = 0;
          for (Cursor repeatB : repeatA.fieldIter("repeatB")) {
            repeatCheck(repeatB.i32Iter("repeatB-0"), index % 4, index);
            repeatCheck(repeatB.i32Iter("repeatB-1"), index % 3, index);
            repeatCheck(repeatB.i32Iter("repeatB-2"), index % 2, index);
            repeatCheck(repeatB.i32Iter("repeatB-3"), index % 3, index);
            repeatCheck(repeatB.i32Iter("repeatB-4"), index % 4, index);

            int repeatCCount = 0;
            for (Cursor repeatC : repeatB.fieldIter("repeatC")) {
              repeatCheck(repeatC.i32Iter("repeatC-0"), index % 2, index);
              repeatCheck(repeatC.i32Iter("repeatC-1"), index % 3, index);
              repeatCheck(repeatC.i32Iter("repeatC-2"), index % 4, index);
              repeatCheck(repeatC.i32Iter("repeatC-3"), index % 5, index);
              repeatCheck(repeatC.i32Iter("repeatC-4"), index % 6, index);
              repeatCheck(repeatC.i32Iter("repeatC-5"), index % 7, index);
              repeatCCount++;
            }

            assertEquals(index % 2, repeatCCount);
            repeatBCount++;
          }

          assertEquals(index % 3, repeatBCount);
          repeatACount++;
        }

        assertEquals(index % 5, repeatACount);
        index++;
      }
    }
  }


  public void generateTestData(WriteTools.ParquetWriteContext context) {
    WriteTools.withParquetWriter(context);
  }

  @Test
  public void testSingleRequiredColumnSchema() throws Exception {
    WriteTools.generateDataAndTest(1, SingleRequiredColumnSchema.class);
  }

  @Test
  public void testSingleOptionalColumn() throws Exception {
    WriteTools.generateDataAndTest(1, SingleOptionalColumnSchema.class);
  }

  @Test
  public void testMultiplyNestedSchema() throws Exception {
    WriteTools.generateDataAndTest(1, MultiplyNestedOptionalSchema.class);
  }

  @Test
  public void testFizzBuzzSchema() throws Exception {
    WriteTools.generateDataAndTest(1, FizzBuzzSchema.class);
  }

  @Test
  public void testSingleRepeatSchema() throws Exception {
    WriteTools.generateDataAndTest(1, SingleRepeatColumnSchema.class);
  }

  @Test
  public void testGroupRepetitionSchema() throws Exception {
    WriteTools.generateDataAndTest(1, GroupRepetitionSchema.class);
  }

  @Test
  public void testLongListSchema() throws Exception {
    WriteTools.generateDataAndTest(1, LongListSchema.class);
  }

  @Test
  public void testMultiplePrimitiveRepeatingColumnSchema() throws Exception {
    WriteTools.generateDataAndTest(1, MultipleRepeatingColumnPrimitiveColumnSchema.class);
  }

  @Test
  public void testSingleNestedRepeatSchema() throws Exception {
    WriteTools.generateDataAndTest(1, SingleNestedRepeatSchema.class);
  }

  @Test
  public void testMultiplyNestedMultiLevelSchema() throws Exception {
    WriteTools.generateDataAndTest(1, MultiplyNestedMultiLevelSchema.class);
  }

}

