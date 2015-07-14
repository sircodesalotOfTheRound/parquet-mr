package org.apache.parquet.parqour.materialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Created by sircodesalot on 7/4/15.
 */
public class ReadSupportMaterializer<T> {
  private final SchemaInfo schemaInfo;
  private final ReadSupport<T> readSupport;

  private final ReadSupport.ReadContext context;
  private final RecordMaterializer<T> materializer;
  private final GroupConverter rootConverter;
  private final MessageType projectionSchema;

  public ReadSupportMaterializer(SchemaInfo schemaInfo, ReadSupport<T> readSupport) {
    this.schemaInfo = schemaInfo;
    this.readSupport = readSupport;
    this.projectionSchema = schemaInfo.projectionSchema();
    this.context = initializeContext(schemaInfo, readSupport);
    this.materializer = initializeMaterializer(schemaInfo, readSupport, context);
    this.rootConverter = materializer.getRootConverter();
  }

  private ReadSupport.ReadContext initializeContext(SchemaInfo schemaInfo, ReadSupport<T> readSupport) {
    Configuration configuration = schemaInfo.configuration();
    Map<String, Set<String>> metadataEntries = captureKeyValueMetadata(schemaInfo.metadata());
    InitContext context = new InitContext(configuration, metadataEntries, schemaInfo.projectionSchema());

    return readSupport.init(context);
  }

  private RecordMaterializer<T> initializeMaterializer(SchemaInfo schemaInfo, ReadSupport<T> readSupport, ReadSupport.ReadContext context) {
    return readSupport.prepareForRead(schemaInfo.configuration(),
      schemaInfo.metadata().getFileMetaData().getKeyValueMetaData(),
      schemaInfo.schema(), context);
  }

  private Map<String, Set<String>> captureKeyValueMetadata(ParquetMetadata metadata) {
    Map<String, String> metadataEntries = metadata.getFileMetaData().getKeyValueMetaData();
    Map<String, Set<String>> multimap = new HashMap<String, Set<String>>();

    for (String key : metadataEntries.keySet()) {
      Set<String> set = new HashSet<String>();
      set.add(metadataEntries.get(key));
      multimap.put(key, set);
    }

    return multimap;
  }

  public T materializeRecord(Cursor cursor) {
    processGroup(cursor, rootConverter.asGroupConverter());
    return materializer.getCurrentRecord();
  }

  private void processGroup(Cursor fieldCursor, GroupConverter groupConverter) {
    groupConverter.start();

    int fieldIndex = 0;
    for (Type field : projectionSchema.getFields()) {
      if (!field.isPrimitive()) {
        throw new NotImplementedException();
      } else {
        PrimitiveType fieldAsPrimitive = field.asPrimitiveType();
        PrimitiveConverter converter = groupConverter.getConverter(fieldIndex).asPrimitiveConverter();
        switch (fieldAsPrimitive.getPrimitiveTypeName()) {
          case INT32:
            Integer value = fieldCursor.i32(field.getName());
            converter.addInt(value);
            break;

          default:
            throw new NotImplementedException();
        }
      }

      fieldIndex++;
    }

    groupConverter.end();

  }
}
