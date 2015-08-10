package org.apache.parquet.parqour.ingest.disk.files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.parqour.exceptions.DataIngestException;

import java.io.IOException;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class HDFSParquetFile implements AutoCloseable {
  private final Configuration configuration;
  private final String path;
  private final Path fsPath;
  private final FileSystem fileSystem;
  private final FileStatus status;
  private FSDataInputStream stream;

  public HDFSParquetFile(Configuration configuration, String path) {
    this.configuration = configuration;
    this.path = path;
    this.fsPath = new Path(path);
    this.fileSystem = determineFileSystem(configuration, fsPath);
    this.stream = this.open(fileSystem, fsPath);
    this.status = this.status(fileSystem, fsPath);
  }

  private FileSystem determineFileSystem(Configuration configuration, Path path) {
    try {
      return path.getFileSystem(configuration);
    } catch (IOException ex) {
      throw new DataIngestException("Unable to open path '%s'", fsPath);
    }
  }

  private FSDataInputStream open(FileSystem fileSystem, Path fsPath) {
    try {
      return fileSystem.open(this.fsPath);
    }  catch (IOException ex ) {
      throw new DataIngestException("Unable to open path '%s'", fsPath);
    }
  }

  private FileStatus status(FileSystem fileSystem, Path fsPath) {
    try {
      return fileSystem.getFileStatus(fsPath);
    }  catch (IOException ex ) {
      throw new DataIngestException("Unable to open path '%s'", fsPath);
    }
  }

  @Override
  public void close() throws Exception {
    stream.close();
  }

  public Configuration configuration() { return this.configuration; }
  public String path() { return this.path; }
  public Path fsPath() { return this.fsPath; }
  public FileSystem fileSystem() { return this.fileSystem; }
  public FileStatus status() { return this.status; }

  public FSDataInputStream stream() { return this.stream; }
}
