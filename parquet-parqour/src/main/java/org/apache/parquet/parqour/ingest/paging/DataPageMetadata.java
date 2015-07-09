package org.apache.parquet.parqour.ingest.paging;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.schema.PrimitiveType;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

/**
 * Created by sircodesalot on 6/15/15.
 */
public class DataPageMetadata {
  private final byte[] data;
  private final int totalItems;
  private final DictionaryPage dictionaryPage;
  private final DataPage page;
  private final DataPageVersion pageVersion;
  private final ColumnDescriptor columnDescriptor;
  private final int startingEntryNumber;
  private final DataPageEncodings encodings;

  private final ReadOffsetCalculator offsetCalculator;

  public DataPageMetadata(DataPage page, DictionaryPage dictionaryPage, ColumnDescriptor columnDescriptor, int startingEntryNumber) {
    this.page = page;
    this.data = captureData();
    this.startingEntryNumber = startingEntryNumber;
    this.dictionaryPage = dictionaryPage;
    this.pageVersion = determinePageVersion(page);
    this.columnDescriptor = columnDescriptor;
    this.totalItems = page.getValueCount();
    this.encodings = new DataPageEncodings(page);

    this.offsetCalculator = new ReadOffsetCalculator(pageVersion, data, columnDescriptor);
  }

  private DataPageVersion determinePageVersion(DataPage page) {
    return page.accept(new DataPage.Visitor<DataPageVersion>() {
      @Override
      public DataPageVersion visit(DataPageV1 dataPageV1) {
        return DataPageVersion.DATA_PAGE_VERSION_1_0;
      }

      @Override
      public DataPageVersion visit(DataPageV2 dataPageV2) {
        return DataPageVersion.DATA_PAGE_VERSION_2_0;
      }
    });
  }

  private byte[] captureData() {
    return page.accept(new DataPage.Visitor<byte[]>() {
      @Override
      public byte[] visit(DataPageV1 dataPageV1) {
        try {
          return dataPageV1.getBytes().toByteArray();
        } catch (IOException e) {
          throw new DataIngestException("Unable to collect page data");
        }
      }

      @Override
      public byte[] visit(DataPageV2 dataPageV2) {
        try {
          return dataPageV2.getData().toByteArray();
        } catch (IOException e) {
          throw new DataIngestException("Unable to collect page data");
        }
      }
    });
  }

  public int computeOffset(DataPageMetadata metadata, ValuesType type) {
    switch (metadata.version()) {
      case DATA_PAGE_VERSION_1_0:
        return computeOffsetParquet1(type);
      case DATA_PAGE_VERSION_2_0:
        return 0;

      default:
        throw new DataIngestException("Invalid Version Number");
    }
  }

  private int computeOffsetParquet1(ValuesType type) {
    switch (type) {
      case DEFINITION_LEVEL:
        return definitionLevelDataOffset();
      case REPETITION_LEVEL:
        return repetitionLevelDataOffset();
      case VALUES:
        return contentDataOffset();

      default:
        throw new DataIngestException("Invalid Reader Type");
    }
  }

  public int computeMaximumValueForValueType(ValuesType type) {
    switch (type) {
      case DEFINITION_LEVEL:
        return maxDefinitionLevel();
      case REPETITION_LEVEL:
        return maxRepetitionLevel();

      default:
        throw new DataIngestException("Invalid Reader Type");
    }
  }

  public byte[] getParquet2RLOrDLChannelData(final ValuesType type) {
    return page.accept(new DataPage.Visitor<byte[]>() {
      @Override
      public byte[] visit(DataPageV1 dataPageV1) {
        throw new DataIngestException("This data only available for Parquet-2");
      }

      @Override
      public byte[] visit(DataPageV2 dataPageV2) {
        try {
          switch (type) {
            case DEFINITION_LEVEL:
              return dataPageV2.getDefinitionLevels().toByteArray();
            case REPETITION_LEVEL:
              return dataPageV2.getRepetitionLevels().toByteArray();

            default:
              throw new NotImplementedException();
          }
        } catch (IOException ex) {
          throw new DataIngestException("Unable to resolve reader");
        }
      }
    });
  }

  public boolean entryNumber(int entryNumber) {
    return entryNumber >= startingEntryNumber && entryNumber <= finalEntryNumber();
  }

  public byte[] data() { return this.data; }
  public int totalItems() { return this.totalItems; }
  public DataPageVersion version() { return this.pageVersion; }
  public ColumnDescriptor columnDescriptor() { return this.columnDescriptor; }
  public DataPage page() { return this.page; }
  public PrimitiveType.PrimitiveTypeName columnType() { return this.columnDescriptor.getType(); }

  public boolean hasDictionaryPage() { return this.dictionaryPage != null; }
  public DictionaryPage dictionaryPage() { return this.dictionaryPage; }

  public int definitionLevelDataOffset() { return offsetCalculator.definitionLevelOffset(); }
  public int repetitionLevelDataOffset() { return offsetCalculator.repetitionLevelOffset(); }
  public int contentDataOffset() { return offsetCalculator.contentOffset(); }

  public int startingEntryNumber() { return this.startingEntryNumber; }
  public int finalEntryNumber() { return this.startingEntryNumber + totalItems - 1; }

  public int maxDefinitionLevel() { return this.columnDescriptor.getMaxDefinitionLevel(); }
  public int maxRepetitionLevel() { return this.columnDescriptor.getMaxRepetitionLevel(); }
  public int typeLength() { return this.columnDescriptor.getTypeLength(); }

  public Encoding definitionLevelEncoding() { return this.encodings.definitionLevelEncoding(); }
  public Encoding repetitionLevelEncoding() { return this.encodings.repetitionLevelEncoding(); }
  public Encoding valuesEncoding() { return this.encodings.valuesEncoding(); }
}

