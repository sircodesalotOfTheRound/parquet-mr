package org.apache.parquet.parqour.ingest.schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.LogicalInverseRewriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class SchemaInfo {
  private final MessageType schema;
  private final Map<String, ColumnDescriptor> columnDescriptorsByPath;
  private final Map<String, Type> schemaNodeByPath;
  private final Configuration configuration;
  private final Path path;
  private final ParquetMetadata metadata;
  private final MessageType projectionSchema;
  private final FilterPredicate predicate;
  private final List<BlockMetaData> blocks;
  private final long totalNumberOfRows;

  public SchemaInfo(Configuration configuration, Path path, ParquetMetadata metadata, GroupType projectionSchema) {
    this(configuration, path, metadata, projectionSchema, null);
  }

  public SchemaInfo(Configuration configuration, Path path, ParquetMetadata metadata, GroupType projectionSchema, FilterPredicate predicate) {
    this.configuration = configuration;
    this.path = path;
    this.metadata = metadata;
    this.projectionSchema = toMessageType(projectionSchema);
    this.predicate = predicate;
    this.schema = metadata.getFileMetaData().getSchema();
    this.columnDescriptorsByPath = collectionColumnDescriptors(schema);
    this.schemaNodeByPath = collectSchemaNodesByPath("", metadata.getFileMetaData().getSchema(), new HashMap<String, Type>());
    this.blocks = collectBlocks(metadata, projectionSchema, predicate);
    this.totalNumberOfRows = countNumberOfRows(blocks);
  }


  private MessageType toMessageType(GroupType group) {
    return new MessageType(group.getName(), group.getFields());
  }

  private List<BlockMetaData> collectBlocks(ParquetMetadata metadata, GroupType projectionSchema, FilterPredicate predicate) {
    if (predicate != null) {
      List<BlockMetaData> blocks = metadata.getBlocks();
      FilterPredicate demorganized = LogicalInverseRewriter.rewrite(predicate);
      FilterCompat.Filter adaptedFilter = FilterCompat.get(demorganized);
      MessageType projectionSchemaAsMessage = new MessageType(projectionSchema.getName(), projectionSchema.getFields());

      return RowGroupFilter.filterRowGroups(adaptedFilter, blocks, projectionSchemaAsMessage);

    } else {
      return metadata.getBlocks();
    }
  }

  private Map<String, ColumnDescriptor> collectionColumnDescriptors(MessageType schema) {
    Map<String, ColumnDescriptor> columnDescriptorMap = new HashMap<String, ColumnDescriptor>();
    for (ColumnDescriptor descriptor : schema.getColumns()) {
      ColumnPath path = ColumnPath.get(descriptor.getPath());
      columnDescriptorMap.put(path.toDotString(), descriptor);
    }

    return columnDescriptorMap;
  }

  private Map<String, Type> collectSchemaNodesByPath(String parentPath, GroupType groupSchema, Map<String, Type> results) {
    for (Type child : groupSchema.getFields()) {
      String path = computePath(parentPath, child.getName());
      if (child.isPrimitive()) {
        results.put(path, child);
      } else {
        collectSchemaNodesByPath(path, (GroupType)child, results);
      }
    }

    return results;
  }

  public int determineRepeatLevel(IngestNode current) {
    return determineRepeatLevel(current, -1);
  }

  private int determineRepeatLevel(IngestNode current, int level) {
    if (current.repetitionType() == Type.Repetition.REPEATED) {
       level++;
    }

    if (current.hasParent()) {
      return determineRepeatLevel(current.parent(), level);
    } else {
      return level;
    }
  }

  public int getDefinitionLevel(String path) {
    if (path.isEmpty()) {
      return 0;
    } else {
      return schema.getMaxDefinitionLevel(path.split("\\."));
    }
  }

  public ColumnDescriptor getColumnDescriptorByPath(String path) {
    if (columnDescriptorsByPath.containsKey(path)) {
      return columnDescriptorsByPath.get(path);
    } else {
      throw new DataIngestException("Unable to find column %s", path);
    }
  }

  public Type getSchemaNodeByPath(String path) {
    if (schemaNodeByPath.containsKey(path)) {
      return schemaNodeByPath.get(path);
    } else {
      throw new DataIngestException("Unable to find node %s", path);
    }
  }

  public static String computePath(String parentPath, String path) {
    if (parentPath.isEmpty()) {
      return path;
    } else {
      return String.format("%s.%s", parentPath, path);
    }
  }

  private long countNumberOfRows(List<BlockMetaData> blocks) {
    long totalRowCount = 0;
    for (BlockMetaData block : blocks) {
      totalRowCount += block.getRowCount();
    }

    return totalRowCount;
  }

  public List<ColumnDescriptor> columnDescriptors() {
    return this.schema.getColumns();
  }

  public String name() { return schema.getName(); }

  public long totalRowCount() { return this.totalNumberOfRows; }
  public MessageType schema() { return this.schema; }
  public Configuration configuration() { return this.configuration; }
  public Path path() { return this.path; }
  public ParquetMetadata metadata() { return this.metadata; }
  public List<BlockMetaData> blocks() { return this.blocks; }
  public MessageType projectionSchema() { return this.projectionSchema; }
  public FilterPredicate predicate() { return this.predicate; }
  public boolean hasPredicate() { return this.predicate != null; }
}
