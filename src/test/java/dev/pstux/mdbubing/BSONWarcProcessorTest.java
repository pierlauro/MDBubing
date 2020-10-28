package dev.pstux.mdbubing;

import static dev.pstux.mdbubing.TestCommon.Constants.NUM_VALID_TEST_WARC_RECORDS;
import static dev.pstux.mdbubing.TestCommon.Constants.TEST_WARC_RESOURCE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import it.unimi.di.law.warc.io.UncompressedWarcReader;
import it.unimi.di.law.warc.io.WarcReader;
import it.unimi.di.law.warc.records.AbstractWarcRecord;
import it.unimi.di.law.warc.records.WarcRecord;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.http.Header;
import org.bson.Document;
import org.junit.Test;
import org.mockito.Mockito;

public class BSONWarcProcessorTest {
  private final BSONWarcProcessor processor = BSONWarcProcessor.INSTANCE;

  @Test
  /** Test that all record types supported by BUbiNG are processed */
  public void testSupportedRecordTypes() throws Exception {
    URL url = BSONWarcProcessorTest.class.getResource(TEST_WARC_RESOURCE_PATH);

    String[] expectedWarcTypes = {"warcinfo", "request", "response"};

    WarcReader reader = new UncompressedWarcReader(url.openStream());
    WarcRecord record;
    for (int i = 0; i < NUM_VALID_TEST_WARC_RECORDS; i++) {
      record = reader.read();
      Document d = processor.process(record, 0);

      // Test that the supported types of WARC records are correctly processed
      assertEquals(d.get("WARC-Type"), expectedWarcTypes[i]);
    }
  }

  @Test
  /** Test that all standard WARC headers are properly processed */
  public void testWarcHeaders() throws Exception {
    URL url = BSONWarcProcessorTest.class.getResource(TEST_WARC_RESOURCE_PATH);

    WarcReader reader = new UncompressedWarcReader(url.openStream());
    WarcRecord record;
    for (int i = 0; i < NUM_VALID_TEST_WARC_RECORDS; i++) {
      record = reader.read();
      Header[] headers = record.getWarcHeaders().getAllHeaders();

      Document d = processor.process(record, 0);
      for (Header h : headers) {
        assertEquals(h.getValue(), d.get(h.getName()));
      }
    }
  }

  @Test
  /** Test that all additional WARC headers are properly processed */
  public void testAdditionalHeaders() throws Exception {
    URL url = BSONWarcProcessorTest.class.getResource(TEST_WARC_RESOURCE_PATH);
    boolean foundDuplicateField = false;

    WarcReader reader = new UncompressedWarcReader(url.openStream());
    for (int i = 0; i < NUM_VALID_TEST_WARC_RECORDS; i++) {
      AbstractWarcRecord record = (AbstractWarcRecord) reader.read();
      Header[] headers = record.getAllHeaders();

      Document d = processor.process(record, 0);

      final String field = BSONWarcProcessor.ADDITIONAL_HEADERS_MDB_FIELD_NAME;

      @SuppressWarnings("unchecked")
      HashMap<String, Object> map = (HashMap<String, Object>) d.get(field);

      for (Header h : headers) {
        String headerValue = h.getValue();
        Object value = map.get(h.getName());
        if (value instanceof String) {
          assertEquals(headerValue, value);
        } else if (value instanceof ArrayList) {
          // There could be duplicate header names (e.g. multiple Set-Cookie)
          assertTrue(ArrayList.class.cast(value).contains(headerValue));
          foundDuplicateField = true;
        }
      }
    }

    // Canary ensuring at least one duplicated header was encountered/tested
    assertTrue(foundDuplicateField);
  }

  @Test
  /** Test that the processor doesn't process records with payload size >= 16MB */
  public void testRecordSizeExceedsMDBLimit() {
    WarcRecord hugeRecord = Mockito.mock(WarcRecord.class);
    long sizeLimit = BSONWarcProcessor.MDB_DOCUMENT_SIZE_LIMIT;
    Mockito.doReturn(sizeLimit).when(hugeRecord).getWarcContentLength();
    assertNull(processor.process(hugeRecord, 0));
  }
}
