package org.apache.parquet.parqour.ingest.ffreader;

import org.apache.parquet.column.ValuesType;
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
  protected long currentEntryNumber;
  protected long totalItemsOnPage;

  protected byte[] data;
  protected int dataOffset;

  public FastForwardReaderBase(DataPageMetadata metadata, ValuesType type) {
    this.metadata = metadata;
    this.type = type;
    this.data = metadata.data();
    this.totalItemsOnPage = metadata.totalItems();
    this.dataOffset = metadata.computeOffset(metadata, type) - 1;

    this.currentEntryNumber = 0;
  }

  public void advanceRowNumber() {
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

  private static FastForwardReaderBase resolveRLEValuesReader(DataPageMetadata metadata) {
    switch (metadata.columnType()) {
      case BOOLEAN:
        return new RLEBooleanFastForwardReader(metadata, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }

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

  private static FastForwardReaderBase resolveDeltaBinaryPackedValuesReader(DataPageMetadata metadata) {
    switch (metadata.columnType()) {
      case INT32:
        return new DeltaPackedIntegerFastForwardReader(metadata, ValuesType.VALUES);

      default:
        throw new NotImplementedException();
    }
  }
}
