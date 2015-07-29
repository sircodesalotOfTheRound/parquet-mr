package org.apache.parquet.parqour.materialization.readsupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
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
  private final QueryInfo queryInfo;
  private final ReadSupport<T> readSupport;

  private final ReadSupport.ReadContext context;
  private final RecordMaterializer<T> materializer;
  private final GroupConverter rootConverter;
  private final MessageType projectionSchema;

  public ReadSupportMaterializer(QueryInfo queryInfo, ReadSupport<T> readSupport) {
    this.queryInfo = queryInfo;
    this.readSupport = readSupport;
    this.projectionSchema = queryInfo.projectionSchema();
    this.context = initializeContext(queryInfo, readSupport);
    this.materializer = initializeMaterializer(queryInfo, readSupport, context);
    this.rootConverter = materializer.getRootConverter();
  }

  private ReadSupport.ReadContext initializeContext(QueryInfo queryInfo, ReadSupport<T> readSupport) {
    Configuration configuration = queryInfo.configuration();
    Map<String, Set<String>> metadataEntries = captureKeyValueMetadata(queryInfo.metadata());
    InitContext context = new InitContext(configuration, metadataEntries, queryInfo.projectionSchema());

    return readSupport.init(context);
  }

  private RecordMaterializer<T> initializeMaterializer(QueryInfo queryInfo, ReadSupport<T> readSupport, ReadSupport.ReadContext context) {
    return readSupport.prepareForRead(queryInfo.configuration(),
      queryInfo.metadata().getFileMetaData().getKeyValueMetaData(),
      queryInfo.schema(), context);
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
