package org.apache.parquet.parqour.query.columns;

import org.apache.parquet.parqour.ingest.schema.SchemaIntersection;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryColumnCollectingVisitor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.apache.parquet.parqour.testtools.TestTools.CONTACTS_SCHEMA;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
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
      new GroupType(REPEATED, "contacts",
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
      new PrimitiveType(REPEATED, BINARY, "ownerPhoneNumbers"),
      new GroupType(REPEATED, "contacts",
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
      new GroupType(REPEATED, "contacts",
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
      new PrimitiveType(REPEATED, BINARY, "ownerPhoneNumbers"),
      new GroupType(REPEATED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testOwnerAndPhoneNumberInUDF() {
    TextQueryColumnCollectingVisitor columnSet
      = createColumnCollectorFromString("select udf(owner) where function(contacts.phoneNumber)");

    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, columnSet.columns());
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "owner"),
      new GroupType(REPEATED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

    assertEquals(shouldBeSchema, subSchema);
  }


  @Test
  public void testOwnerPhoneNumbersAndContactsInAlias() {
    TextQueryColumnCollectingVisitor columnSet
      = createColumnCollectorFromString("select ('name: ' + contacts.name) as theName where count(ownerPhoneNumbers) > 10");

    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, columnSet.columns());
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REPEATED, BINARY, "ownerPhoneNumbers"),
      new GroupType(REPEATED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "name")));

    assertEquals(shouldBeSchema, subSchema);
  }

  private TextQueryColumnCollectingVisitor createColumnCollectorFromString(String expression) {
    TextQueryLexer lexer = new TextQueryLexer(expression, true);
    TextQueryTreeRootExpression rootExpression = new TextQueryTreeRootExpression(lexer);

    return new TextQueryColumnCollectingVisitor(rootExpression);
  }
}
