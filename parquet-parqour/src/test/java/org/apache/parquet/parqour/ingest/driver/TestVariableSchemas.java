package org.apache.parquet.parqour.ingest.driver;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.testtools.ParquetConfiguration;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.parqour.testtools.WriteTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/22/15.
 */
public class TestVariableSchemas {
  private static final int TOTAL_ROWS = TestTools.generateRandomInt(10000);

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

  /*private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multipliers",
    new PrimitiveType(REQUIRED, INT32, "one"),
    /*new PrimitiveType(OPTIONAL, INT32, "two"),
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
          new PrimitiveType(REPEATED, INT32, "repeatC-5")))),
    new PrimitiveType(OPTIONAL, INT32, "last"));*/

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

/*
      private final GroupType COUNTING_SCHEMA = new GroupType(REQUIRED, "multipliers",
        new PrimitiveType(REQUIRED, INT32, "one"));

      @Override
      public void write(ParquetWriter<Group> writer) throws IOException {
        for (int index = 0; index < TOTAL_ROWS; index++) {
          Group schema = new SimpleGroup(COUNTING_SCHEMA);

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
              //repeatAdd(repeatB, "repeatB-0", index % 4, index);
              repeatAdd(repeatB, "repeatB-1", index % 3, index);
              repeatAdd(repeatB, "repeatB-2", index % 2, index);
              repeatAdd(repeatB, "repeatB-3", index % 3, index);
              repeatAdd(repeatB, "repeatB-4", index % 4, index);

              for (int repeatCIndex = 0; repeatCIndex < index % 2; repeatCIndex++) {
                Group repeatC = repeatB.addGroup("repeatC");
                //repeatAdd(repeatC, "repeatC-0", index % 3, index);
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

      private void repeatAdd(Group group, String field, int times, int startValue) {
        for (int index = 0; index < times; index++) {
          group.append(field, index + startValue);
        }
      }
    });
  }


  @Test
  public void testVariableSchemas() throws Exception {
    for (ParquetConfiguration configuration : TestTools.CONFIGURATIONS) {
      this.generateTestData(configuration);

      TestTools.repeat(1, new TestTools.RepeatCallback() {
        @Override
        public void execute() throws Exception {
          int index = 0;
          for (Cursor cursor : Parqour.query(TestTools.TEST_FILE_PATH)) {
            testFirstField(index, cursor);
            testSecondField(index, cursor);
            testThirdField(index, cursor);
            testFourthField(index, cursor);
            testFifthField(index, cursor);
            testSixthField(index, cursor);

            index ++;
          }
        }
      });
    }
  }

  private void testFirstField(int index, Cursor cursor) {
    assertEquals((Integer) index, cursor.i32("one"));
  }

  private void testSecondField(int index, Cursor cursor) {
    // Should exist only if not null.
    if (index % 2 == 0) {
      assertEquals((Integer) (index * 2), cursor.i32(FIELD_NUMBER_TWO));
    } else {
      assertNull(cursor.i32(FIELD_NUMBER_TWO));
    }
  }

  private void testThirdField(int index, Cursor cursor) {
    // Should repeat.
    int repeat = 0;
    for (int value : cursor.i32iter(2)) {
      assertEquals((index * 3) + repeat, value);
      repeat++;
    }
    assertEquals((index % 5) + 1, repeat);
  }

  private void testFourthField(Integer index, Cursor cursor) {
    if (index % 3 == 0 || index % 5 == 0) {
      if (index % 3 == 0) {
        assertEquals(index, cursor.field("fizz_buzz").i32("fizz"));
      } else {
        assertNull(cursor.field("fizz_buzz").field(0));
      }

      if (index % 5 == 0) {
        assertEquals(index, cursor.field("fizz_buzz").i32("buzz"));
      } else {
        assertNull(cursor.field("fizz_buzz").field(1));
      }
    } else {
      assertNull(cursor.field(3));
    }
  }

  private void testFifthField(Integer index, Cursor cursor) {
    if (index % 2 == 0) {
      Cursor first = cursor.field(4);
      repeatCheck(first.i32iter(0), index % 3, index);
      repeatCheck(first.i32iter(1), index % 5, index);
      repeatCheck(first.i32iter(2), index % 7, index);

      assertNotNull(first);
      if (index % 3 == 0) {
        Cursor second = first.field(3);
        repeatCheck(second.i32iter(0), index % 4, index);
        repeatCheck(second.i32iter(1), index % 5, index);
        repeatCheck(second.i32iter(2), index % 6, index);
        repeatCheck(second.i32iter(3), index % 7, index);
        repeatCheck(second.i32iter(4), index % 8, index);

        assertNotNull(second);
        if (index % 4 == 0) {
          Cursor third = second.field(5);
          assertNotNull(third);

          repeatCheck(third.i32iter(0), index % 2, index);
          repeatCheck(third.i32iter(1), index % 3, index);
          repeatCheck(third.i32iter(2), index % 4, index);
          repeatCheck(third.i32iter(3), index % 5, index);
          repeatCheck(third.i32iter(4), index % 6, index);
          repeatCheck(third.i32iter(5), index % 7, index);

        } else {
          assertNull(second.field(5));
        }
      } else {
        assertNull(first.field(3));
      }
    } else {
      assertNull(cursor.field(4));
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

  private void testSixthField(Integer index, Cursor cursor) {
    int repeatACount = 0;
    for (Cursor repeatA : cursor.fieldIter(5)) {
      repeatCheck(repeatA.i32iter(0), index % 2, index);
      repeatCheck(repeatA.i32iter(1), index % 3, index);
      repeatCheck(repeatA.i32iter(2), index % 4, index);

      int repeatBCount = 0;
      for (Cursor repeatB : repeatA.fieldIter(3)) {
        repeatCheck(repeatB.i32iter(1), index % 3, index);
        repeatCheck(repeatB.i32iter(2), index % 2, index);
        repeatCheck(repeatB.i32iter(3), index % 3, index);
        repeatCheck(repeatB.i32iter(4), index % 4, index);

        int repeatCCount = 0;
        for (Cursor repeatC : repeatB.fieldIter(5)) {

          repeatCheck(repeatC.i32iter(1), index % 5, index);
          repeatCheck(repeatC.i32iter(2), index % 2, index);
          repeatCheck(repeatC.i32iter(3), index % 5, index);
          repeatCheck(repeatC.i32iter(4), index % 3, index);
          repeatCCount++;
        }

        assertEquals(index % 2, repeatCCount);
        repeatBCount++;
      }

      assertEquals(index % 3, repeatBCount);
      repeatACount++;
    }

    assertEquals(index % 5, repeatACount);
  }
  */
}