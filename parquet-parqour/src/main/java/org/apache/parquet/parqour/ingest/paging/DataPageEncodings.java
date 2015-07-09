package org.apache.parquet.parqour.ingest.paging;


import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;

/**
 * Created by sircodesalot on 6/17/15.
 */
public class DataPageEncodings {
  private final Encoding definitionLevelEncoding;
  private final Encoding repetitionLevelEncoding;
  private final Encoding valuesEncoding;

  public DataPageEncodings(DataPage page) {
    this.definitionLevelEncoding = determineDefinitionLevelEncoding(page);
    this.repetitionLevelEncoding = determineRepetitionLevelEncoding(page);
    this.valuesEncoding = determineValuesEncoding(page);
  }

  private Encoding determineDefinitionLevelEncoding(DataPage page) {
    return page.accept(new DataPage.Visitor<Encoding>() {
      @Override
      public Encoding visit(DataPageV1 dataPageV1) {
        return dataPageV1.getDlEncoding();
      }

      @Override
      public Encoding visit(DataPageV2 dataPageV2) {
        return Encoding.RLE;
      }
    });
  }

  private Encoding determineRepetitionLevelEncoding(DataPage page) {
    return page.accept(new DataPage.Visitor<Encoding>() {
      @Override
      public Encoding visit(DataPageV1 dataPageV1) {
        return dataPageV1.getRlEncoding();
      }

      @Override
      public Encoding visit(DataPageV2 dataPageV2) {
        return Encoding.RLE;
      }
    });
  }

  private Encoding determineValuesEncoding(DataPage page) {
    return page.accept(new DataPage.Visitor<Encoding>() {
      @Override
      public Encoding visit(DataPageV1 dataPageV1) {
        return dataPageV1.getValueEncoding();
      }

      @Override
      public Encoding visit(DataPageV2 dataPageV2) {
        return dataPageV2.getDataEncoding();
      }
    });
  }

  public Encoding definitionLevelEncoding() { return this.definitionLevelEncoding; }
  public Encoding repetitionLevelEncoding() { return this.repetitionLevelEncoding; }
  public Encoding valuesEncoding() { return this.valuesEncoding; }
}
