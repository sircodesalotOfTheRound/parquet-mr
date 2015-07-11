package org.apache.parquet.parqour.analysis.subschema;

import org.apache.parquet.parqour.ingest.schema.SchemaIntersection;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryColumnCollectingVisitor;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.apache.parquet.parqour.testtools.TestTools.CONTACTS_SCHEMA;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 7/11/15.
 */
public class TestColumnCollection {
  @Test
  public void testWildcardWithColumnCollection() {
    TextQueryColumnCollectingVisitor columnSet = createColumnCollectorFromString("select *");
    assertTrue(columnSet.containsAWildcardExpression());
  }

  @Test
  public void testSingleItemSchemaUsingColumnCollection() {
    TextQueryColumnCollectingVisitor columnSet = createColumnCollectorFromString("select owner");
    Iterable<String> subschema = columnSet.columns();

    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, subschema);
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "owner"));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testContactsUsingColumnCollection() {
    TextQueryColumnCollectingVisitor columnSet = createColumnCollectorFromString("select contacts");
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, columnSet.columns());
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "name"),
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testPhoneNumbersWithOwnerPhoneNumbersUsingColumnCollection() {
    TextQueryColumnCollectingVisitor columnSet = createColumnCollectorFromString("select contacts.phoneNumber, ownerPhoneNumbers");
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, columnSet.columns());
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "ownerPhoneNumbers"),
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testOwnerAndNameUsingColumnCollection() {
    TextQueryColumnCollectingVisitor columnSet = createColumnCollectorFromString("select owner, contacts.name");
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, columnSet.columns());
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "owner"),
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "name")));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testOwnerAndNameUsingWhereStatement() {
    TextQueryColumnCollectingVisitor columnSet
      = createColumnCollectorFromString("select ownerPhoneNumbers where contacts.phoneNumber = 12345");

    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, columnSet.columns());
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "ownerPhoneNumbers"),
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

    assertEquals(shouldBeSchema, subSchema);
  }

  private TextQueryColumnCollectingVisitor createColumnCollectorFromString(String expression) {
    ParquelLexer lexer = new ParquelLexer(expression, true);
    TextQueryTreeRootExpression rootExpression = new TextQueryTreeRootExpression(lexer);

    return new TextQueryColumnCollectingVisitor(rootExpression);
  }
}
