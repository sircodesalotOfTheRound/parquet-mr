package org.apache.parquet.parqour.ingest.schema;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by sircodesalot on 6/21/15.
 */
public class SchemaIntersection {
  private final GroupType subSchema;

  public SchemaIntersection(MessageType masterSchema, String ... paths) {
    this.subSchema = computeSubschema(masterSchema, paths);
  }

  public SchemaIntersection(MessageType masterSchema, Iterable<String> paths) {
    this.subSchema = computeSubschema(masterSchema, paths);
  }

  private GroupType computeSubschema(MessageType masterSchema, Iterable<String> paths) {
    List<String> pathList = new ArrayList<String>();
    for (String path : paths) {
      pathList.add(path);
    }

    String[] pathsArray = new String[pathList.size()];
    return computeSubschema(masterSchema, pathList.toArray(pathsArray));
  }

  private GroupType computeSubschema(MessageType masterSchema, String[] paths) {
    // Traverse automatically updates the path-set.
    Set<String> pathSet = pathsAsSet(paths);
    for (Type field : masterSchema.getFields()) {
      traverse(field, "", pathSet);
    }

    return buildSubSchema(masterSchema, pathSet);
  }

  private Set<String> pathsAsSet(String[] paths) {
    Set<String> set = new HashSet<String>();
    for (String path : paths) {
      set.add(path);
    }

    return set;
  }

  // Traverse and find the paths containing items in the set.
  private boolean traverse(Type node, String parentPath, Set<String> paths) {
    String path = computePath(parentPath, node.getName());
    // (1) If the path is in the list, then add it, and all of it's children.
    if (paths.contains(path)) {
      paths = addChildrenToPathList(node, parentPath, paths);
    }

    // (2) If we're at a leaf, then fall through and return.
    // (3) If we're at a group, then traverse to all of it's children.
    // if one of the children happens to be on the list, add this node to
    // the list as well.
    if (!node.isPrimitive()) {
      GroupType group = (GroupType) node;
      for (Type field : group.getFields()) {
        if (traverse(field, path, paths)) {
          paths.add(path);
        }
      }
    }

    // If this node is on the list, return true.
    return paths.contains(path);
  }

  // If we find a node that is in the chosen path list, then we should add all of its children as well.
  private Set<String> addChildrenToPathList(Type node, String parentPath, Set<String> paths) {
    String path = computePath(parentPath, node.getName());
    paths.add(path);

    if (!node.isPrimitive()) {
      GroupType group = (GroupType)node;
      for (Type field : group.getFields()) {
        paths = addChildrenToPathList(field, path, paths);
      }
    }

    return paths;
  }

  public static String computePath(String parentPath, String path) {
    if (parentPath.isEmpty()) {
      return path;
    } else {
      return String.format("%s.%s", parentPath, path);
    }
  }

  public MessageType buildSubSchema(MessageType masterSchema, Set<String> paths) {
    List<Type> childrenInSubSchema = new ArrayList<Type>();
    for (Type child : masterSchema.getFields()) {
      Type result = buildSubSchema(child, "", paths);
      if (result != null) {
        childrenInSubSchema.add(result);
      }
    }

    return new MessageType(masterSchema.getName(), childrenInSubSchema);
  }

  public Type buildSubSchema(Type node, String parentPath, Set<String> paths) {
    // If the path is not in the list, then stop descending.
    // If the node is primitive, then return this node.
    String path = computePath(parentPath, node.getName());
    if (!paths.contains(path)) return null;
    if (node.isPrimitive()) return node;

    // Otherwise, we need to build a new subgroup.
    // Descend and add any subnodes that are part of the group.
    List<Type> childrenInSubschema = new ArrayList<Type>();
    GroupType group = (GroupType)node;
    for (Type child : group.getFields()) {
      Type result = buildSubSchema(child, path, paths);
      if (result != null) {
        childrenInSubschema.add(child);
      }
    }

    return new GroupType(node.getRepetition(), node.getName(), childrenInSubschema);
  }

  public GroupType subSchema() {
    return this.subSchema;
  }
}
