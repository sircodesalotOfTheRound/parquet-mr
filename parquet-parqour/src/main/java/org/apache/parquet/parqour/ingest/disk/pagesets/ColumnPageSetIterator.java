package org.apache.parquet.parqour.ingest.disk.pagesets;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.DictionaryPageInfo;
import org.apache.parquet.parqour.ingest.disk.pages.PageInfo;
import org.apache.parquet.parqour.ingest.disk.pages.meta.PageMeta;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by sircodesalot on 8/11/15.
 */
public class ColumnPageSetIterator implements Iterator<PageMeta> {
  private final RowGroupColumnPageSetInfo columnInfo;
  private final HDFSParquetFile file;
  private final HDFSParquetFileMetadata metadata;
  private final FSDataInputStream stream;
  private final long totalEntryCount;

  private long currentOffset;
  private long totalEntriesRead;

  private DictionaryPageInfo dictionaryPageInfo;

  public ColumnPageSetIterator(RowGroupColumnPageSetInfo columnInfo, HDFSParquetFile file, HDFSParquetFileMetadata metadata) {
    this.file = file;
    this.metadata = metadata;
    this.stream = file.stream();
    this.dictionaryPageInfo = null;
    this.columnInfo = columnInfo;
    this.currentOffset = columnInfo.startingOffset();

    this.totalEntriesRead = 0;
    this.totalEntryCount = columnInfo.totalEntryCount();
  }

  @Override
  public boolean hasNext() {
    return (totalEntriesRead < totalEntryCount);
  }

  @Override
  public PageMeta next() {
    return nextPageMeta();
  }

  private PageMeta nextPageMeta() {
    try {
      stream.seek(currentOffset);

      PageHeader header = Util.readPageHeader(stream);
      int pageStartingOffset = (int) (stream.getPos() - currentOffset);

      DataSlate slate = new DataSlate(file, pageStartingOffset);
      PageInfo pageInfo = PageInfo.readPage(columnInfo, metadata, header, slate, pageStartingOffset);

      PageMeta pageMeta = new PageMeta(header, pageInfo);
      if (pageInfo.isDictionaryPage()) {
        this.dictionaryPageInfo = (DictionaryPageInfo) pageInfo;
      } else {
        slate.addSegment(stream, header);
        totalEntriesRead += pageInfo.entryCount();
      }

      currentOffset = (stream.getPos() + header.getCompressed_page_size());
      return pageMeta;
    } catch (IOException ex) {
      throw new DataIngestException("Unable to read pages for column: '%s'", columnInfo.path());
    }
  }
}
