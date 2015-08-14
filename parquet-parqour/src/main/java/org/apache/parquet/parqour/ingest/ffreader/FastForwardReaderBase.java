package org.apache.parquet.parqour.ingest.ffreader;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.disk.pages.info.DataPageInfo;
import org.apache.parquet.parqour.ingest.ffreader.delta.DeltaByteArrayBinaryFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.delta.DeltaPackedIntegerFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.dictionary.*;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.packed.rle.Parquet2RLEBitPackedHybridFastForwardIntReader;
import org.apache.parquet.parqour.ingest.ffreader.packed.rle.RLEBitPackedHybridFastForwardIntReader;
import org.apache.parquet.parqour.ingest.ffreader.packed.rle.RLEBooleanFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.plain.*;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 6/13/15.
 */
public abstract class FastForwardReaderBase implements FastForwardReader {
  protected final DataPageMetadata metadata;
  protected final ValuesType type;
  // TODO: Make this an int.
  protected long currentEntryNumber;
  protected long totalItemsOnPage;

  protected byte[] data;
  protected int dataOffset;

  public FastForwardReaderBase(DataPageInfo info, ValuesType type) {
    this.metadata = null;
    this.type = type;
    this.currentEntryNumber = 0;
    this.totalItemsOnPage = info.entryCount();
    this.data = info.data();
    this.dataOffset = info.computeOffset(type) - 1;
  }

  @Deprecated
  public FastForwardReaderBase(DataPageMetadata metadata, ValuesType type) {
    this.metadata = metadata;
    this.type = type;
    this.data = metadata.data();
    this.totalItemsOnPage = metadata.totalItems();
    this.dataOffset = metadata.computeOffset(metadata, type) - 1;

    this.currentEntryNumber = 0;
  }

  public void advanceEntryNumber() {
    this.currentEntryNumber++;
  }

  @Override
  public final long currentEntryNumber() {
    return this.currentEntryNumber;
  }

  @Override
  public long totalItemsOnPage() { return this.totalItemsOnPage; }

  @Override
  public ValuesType type() { return this.type; }

  @Override
  public boolean isEof() { return this.currentEntryNumber >= totalItemsOnPage;}

  public static FastForwardReaderBase resolve(DataPageInfo info, ValuesType type) {
    switch (type) {
      case DEFINITION_LEVEL:
      case REPETITION_LEVEL:
        return resolveRLEBitPackedValuesReaderFromVersion(info, type);

      case VALUES:
        return resolveValuesReaderFromEncoding(info);

      default:
        throw new NotImplementedException();
    }
  }

  private static FastForwardReaderBase resolveRLEBitPackedValuesReaderFromVersion(DataPageInfo info, ValuesType type) {
    switch (info.version()) {
      case PARQUET_1_0:
        //return new RLEBitPackedHybridFastForwardIntReader(metadata, type);
      case PARQUET_2_0:
        //return new Parquet2RLEBitPackedHybridFastForwardIntReader(metadata, type);

      default:
        throw new NotImplementedException();
    }
  }

  private static FastForwardReaderBase resolveValuesReaderFromEncoding(DataPageInfo info) {
    switch (info.contentEncoding()) {
      case PLAIN:
        return resolvePlainValuesReader(info);

      case PLAIN_DICTIONARY:
        return resolvePlainDictionaryValuesReader(info);

      case RLE:
        return resolveRLEValuesReader(info);

      case RLE_DICTIONARY:
        return resolveRLEDictionaryValuesReader(info);

      case DELTA_BYTE_ARRAY:
        return resolveDeltaByteArrayValuesReader(info);

      /*
      case DELTA_LENGTH_BYTE_ARRAY:
        throw new NotImplementedException();

      */
      case DELTA_BINARY_PACKED:
        return resolveDeltaBinaryPackedValuesReader(info);

      default:
        throw new NotImplementedException();
    }
  }

  private static FastForwardReaderBase resolvePlainValuesReader(DataPageInfo info) {
    switch (info.type()) {
      case INT32:
        return new PlainInt32FastForwardReader(info, ValuesType.VALUES);

      case INT64:
        return new PlainInt64FastForwardReader(info, ValuesType.VALUES);

      case BOOLEAN:
        return new PlainBooleanFastForwardReader(info, ValuesType.VALUES);

      case FLOAT:
        return new PlainSingleFastForwardReader(info, ValuesType.VALUES);

      case DOUBLE:
        return new PlainDoubleFastForwardReader(info, ValuesType.VALUES);

      case BINARY:
        return new PlainBinaryFastForwardReader(info, ValuesType.VALUES);

      case FIXED_LEN_BYTE_ARRAY:
        return new PlainFixedBinaryFastForwardReader(info, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

  private static FastForwardReaderBase resolveRLEValuesReader(DataPageInfo info) {
    switch (info.type()) {
      case BOOLEAN:
        return new RLEBooleanFastForwardReader(info, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

  private static FastForwardReaderBase resolvePlainDictionaryValuesReader(DataPageInfo info) {
    switch (info.type()) {
      case INT32:
        return new Int32DictionaryFastForwardReader(info, ValuesType.VALUES);

      case BINARY:
        return new PlainBinaryDictionaryFastForwardReader(info, ValuesType.VALUES);
/*
      case INT64:
        return new Int64DictionaryFastForwardReader(metadata, ValuesType.VALUES);


      case FIXED_LEN_BYTE_ARRAY:
        return new PlainBinaryDictionaryFastForwardReader(metadata, ValuesType.VALUES);*/

      default:
        throw new NotImplementedException();
    }
  }

  private static FastForwardReaderBase resolveDeltaBinaryPackedValuesReader(DataPageInfo info) {
    switch (info.type()) {
      case INT32:
        return new DeltaPackedIntegerFastForwardReader(info, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

  private static FastForwardReaderBase resolveRLEDictionaryValuesReader(DataPageInfo info) {
    switch (info.type()) {
      case INT32:
        return new Int32DictionaryFastForwardReader(info, ValuesType.VALUES);

      case BINARY:
        return new RLEBinaryDictionaryFastForwardReader(info, ValuesType.VALUES);

      case FIXED_LEN_BYTE_ARRAY:
        return new RLEFixedBinaryDictionaryFastForwardReader(info, ValuesType.VALUES);
      /*
      case INT64:
        return new Int64DictionaryFastForwardReader(metadata, ValuesType.VALUES);

      */
      default:
        throw new NotImplementedException();
    }
  }

  private static FastForwardReaderBase resolveDeltaByteArrayValuesReader(DataPageInfo info) {
    switch (info.type()) {
      case BINARY:
        return new DeltaByteArrayBinaryFastForwardReader(info, ValuesType.VALUES);

      case FIXED_LEN_BYTE_ARRAY:
        return new DeltaByteArrayBinaryFastForwardReader(info, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

  @Deprecated
  public static FastForwardReaderBase resolve(DataPageMetadata metadata, ValuesType type) {
    switch (type) {
      case DEFINITION_LEVEL:
      case REPETITION_LEVEL:
        return resolveRLEBitPackedValuesReaderFromVersion(metadata, type);
      case VALUES:
        return resolveValuesReaderFromEncoding(metadata);

      default:
        throw new NotImplementedException();
    }
  }

  @Deprecated
  private static FastForwardReaderBase resolveRLEBitPackedValuesReaderFromVersion(DataPageMetadata metadata, ValuesType type) {
    switch (metadata.version()) {
      case DATA_PAGE_VERSION_1_0:
        return new RLEBitPackedHybridFastForwardIntReader(metadata, type);
      case DATA_PAGE_VERSION_2_0:
        return new Parquet2RLEBitPackedHybridFastForwardIntReader(metadata, type);

      default:
        throw new NotImplementedException();
    }
  }


  @Deprecated
  private static FastForwardReaderBase resolveValuesReaderFromEncoding(DataPageMetadata metadata) {
    switch (metadata.valuesEncoding()) {
      case PLAIN:
        return resolvePlainValuesReader(metadata);

      case PLAIN_DICTIONARY:
        return resolvePlainDictionaryValuesReader(metadata);

      case RLE:
        return resolveRLEValuesReader(metadata);

      case RLE_DICTIONARY:
        return resolveRLEDictionaryValuesReader(metadata);

      case DELTA_LENGTH_BYTE_ARRAY:
        throw new NotImplementedException();

      case DELTA_BYTE_ARRAY:
        return resolveDeltaByteArrayValuesReader(metadata);

      case DELTA_BINARY_PACKED:
        return resolveDeltaBinaryPackedValuesReader(metadata);

      default:
        throw new NotImplementedException();
    }
  }

  @Deprecated
  private static FastForwardReaderBase resolvePlainValuesReader(DataPageMetadata metadata) {
    switch (metadata.columnType()) {
      case BOOLEAN:
        return new PlainBooleanFastForwardReader(metadata, ValuesType.VALUES);

      case INT32:
        return new PlainInt32FastForwardReader(metadata, ValuesType.VALUES);
      case INT64:
        return new PlainInt64FastForwardReader(metadata, ValuesType.VALUES);

      case FLOAT:
        return new PlainSingleFastForwardReader(metadata, ValuesType.VALUES);
      case DOUBLE:
        return new PlainDoubleFastForwardReader(metadata, ValuesType.VALUES);

      case BINARY:
        return new PlainBinaryFastForwardReader(metadata, ValuesType.VALUES);
      case FIXED_LEN_BYTE_ARRAY:
        return new PlainFixedBinaryFastForwardReader(metadata, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

  @Deprecated
  private static FastForwardReaderBase resolveRLEValuesReader(DataPageMetadata metadata) {
    switch (metadata.columnType()) {
      case BOOLEAN:
        return new RLEBooleanFastForwardReader(metadata, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

  @Deprecated
  private static FastForwardReaderBase resolvePlainDictionaryValuesReader(DataPageMetadata metadata) {
    switch (metadata.columnType()) {
      case INT32:
        return new Int32DictionaryFastForwardReader(metadata, ValuesType.VALUES);

      case INT64:
        return new Int64DictionaryFastForwardReader(metadata, ValuesType.VALUES);

      case BINARY:
        return new PlainBinaryDictionaryFastForwardReader(metadata, ValuesType.VALUES);

      case FIXED_LEN_BYTE_ARRAY:
        return new PlainBinaryDictionaryFastForwardReader(metadata, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

  @Deprecated
  private static FastForwardReaderBase resolveRLEDictionaryValuesReader(DataPageMetadata metadata) {
    switch (metadata.columnType()) {
      case FIXED_LEN_BYTE_ARRAY:
        return new RLEFixedBinaryDictionaryFastForwardReader(metadata, ValuesType.VALUES);
      case BINARY:
        return new RLEBinaryDictionaryFastForwardReader(metadata, ValuesType.VALUES);

      case INT32:
        return new Int32DictionaryFastForwardReader(metadata, ValuesType.VALUES);
      case INT64:
        return new Int64DictionaryFastForwardReader(metadata, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

  @Deprecated
  private static FastForwardReaderBase resolveDeltaByteArrayValuesReader(DataPageMetadata metadata) {
    switch (metadata.columnType()) {
      case BINARY:
        return new DeltaByteArrayBinaryFastForwardReader(metadata, ValuesType.VALUES);

      case FIXED_LEN_BYTE_ARRAY:
        return new DeltaByteArrayBinaryFastForwardReader(metadata, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

  @Deprecated
  private static FastForwardReaderBase resolveDeltaBinaryPackedValuesReader(DataPageMetadata metadata) {
    switch (metadata.columnType()) {
      case INT32:
        return new DeltaPackedIntegerFastForwardReader(metadata, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }
}
