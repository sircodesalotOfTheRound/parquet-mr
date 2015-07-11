package org.apache.parquet.parqour.analysis.subschema;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.ingest.schema.SchemaIntersection;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.column.TextQueryNamedColumnExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryColumnCollectingVisitor;
import org.junit.Test;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import static org.apache.parquet.parqour.testtools.TestTools.CONTACTS_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/21/15.
 */
public class TestSchemaIntersection {


  @Test
  public void testOwnerOnly() {
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, "owner");
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "owner"));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testContacts() {
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, "contacts");
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "name"),
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

    assertEquals(shouldBeSchema, subSchema);
  }


  @Test
  public void testPhoneNumbersWithOwnerPhoneNumbers() {
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, "contacts.phoneNumber", "ownerPhoneNumbers");
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "ownerPhoneNumbers"),
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

    assertEquals(shouldBeSchema, subSchema);
  }


  @Test
  public void testOwnerAndName() {
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, "owner", "contacts.name");
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "owner"),
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "name")));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testSingleItemSchemaUsingTextQuery() {
    Iterable<String> subschema = subschemaColumnsFromString("select owner");
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, subschema);
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "owner"));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testContactsUsingTextQuery() {
    Iterable<String> subschema = subschemaColumnsFromString("select contacts");
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, subschema);
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "name"),
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testPhoneNumbersWithOwnerPhoneNumbersUsingTextQuery() {
    Iterable<String> subschema = subschemaColumnsFromString("select ownerPhoneNumbers, contacts.phoneNumber");
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, subschema);
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "ownerPhoneNumbers"),
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "phoneNumber")));

    assertEquals(shouldBeSchema, subSchema);
  }

  @Test
  public void testOwnerAndNameUsingTextQuery() {
    Iterable<String> subschema = subschemaColumnsFromString("select owner, contacts.name");
    SchemaIntersection intersection = new SchemaIntersection(CONTACTS_SCHEMA, subschema);
    GroupType subSchema = intersection.subSchema();

    MessageType shouldBeSchema = new MessageType("AddressBook",
      new PrimitiveType(REQUIRED, BINARY, "owner"),
      new GroupType(REQUIRED, "contacts",
        new PrimitiveType(REQUIRED, BINARY, "name")));

    assertEquals(shouldBeSchema, subSchema);
  }

  private Iterable<String> subschemaColumnsFromString(String expression) {
    ParquelLexer lexer = new ParquelLexer(expression, true);
    TextQueryTreeRootExpression rootExpression = new TextQueryTreeRootExpression(lexer);

    return rootExpression.asSelectStatement().columnSet().columns()
      .castTo(TextQueryNamedColumnExpression.class)
      .map(new Projection<TextQueryNamedColumnExpression, String>() {
        @Override
        public String apply(TextQueryNamedColumnExpression columnExpression) {
          return columnExpression.identifier().toString();
        }
      });
  }
}
