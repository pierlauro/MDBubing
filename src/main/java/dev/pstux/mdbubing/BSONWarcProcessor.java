package dev.pstux.mdbubing;

import it.unimi.di.law.warc.processors.ParallelFilteredProcessorRunner.Processor;
import it.unimi.di.law.warc.records.AbstractWarcRecord;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.ByteArraySessionOutputBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.Header;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BSONWarcProcessor implements Processor<Document> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BSONWarcProcessor.class);

  public static final BSONWarcProcessor INSTANCE = new BSONWarcProcessor();
  public static final long MDB_DOCUMENT_SIZE_LIMIT = 16 * 1024 * 1024;
  public static final String ADDITIONAL_HEADERS_MDB_FIELD_NAME = "headers";
  public static final String PAYLOAD_MDB_FIELD_NAME = "payload";

  private final ByteArrayOutputStream os = new ByteArrayOutputStream();
  private final ByteArraySessionOutputBuffer buf = new ByteArraySessionOutputBuffer();

  @Override
  public void close() throws IOException {
    os.close();
    buf.close();
  }

  @Override
  public Processor<Document> copy() {
    return INSTANCE;
  }

  @Override
  public Document process(WarcRecord r, long storePosition) {
    // Optimistic assumption: payload size < 16MB --> document size < 16MB.
    // It's not worth it to compute the exact document size for each record.
    // If document size is > 16 MB, the insert in MongoDB will simply fail.
    if (r.getWarcContentLength() >= MDB_DOCUMENT_SIZE_LIMIT) {
      LOGGER.error("Record too big. ID: {} - URI: {}", r.getWarcRecordId(), r.getWarcTargetURI());
      return null;
    }

    Document obj = new Document();
    for (Header h : r.getWarcHeaders().getAllHeaders()) {
      obj.append(h.getName(), h.getValue());
    }

    processAdditionalHeaders(r, obj);

    try {
      // TODO optimize
      String payload;
      synchronized (os) {
        r.write(os, buf);
        payload = os.toString();
      }
      payload = payload.substring(payload.length() - (int) r.getWarcContentLength());
      obj.append(PAYLOAD_MDB_FIELD_NAME, payload);
    } catch (IOException e) {
      LOGGER.error(
          "Couldn't add payload to document. ID: {} - URI: {}",
          r.getWarcRecordId(),
          r.getWarcTargetURI());
      return null;
    }

    return obj;
  }

  void processAdditionalHeaders(WarcRecord r, Document obj) {
    AbstractWarcRecord record = (AbstractWarcRecord) r;

    // There could be duplicate header names (e.g. multiple Set-Cookie)
    Map<String, Object> headers = new HashMap<String, Object>();

    // TODO optimize
    for (Header h : record.getAllHeaders()) {
      String name = h.getName();
      String value = h.getValue();

      Object current = headers.get(name);
      if (current == null) {
        headers.put(name, value);
      } else {
        if (current instanceof String) {
          ArrayList<String> list = new ArrayList<String>();
          list.add((String) current);
          list.add(value);
          headers.put(name, list);
          continue;
        }

        @SuppressWarnings("unchecked")
        ArrayList<String> list = (ArrayList<String>) current;
        list.add(value);
      }
    }

    obj.append(ADDITIONAL_HEADERS_MDB_FIELD_NAME, headers);
  }
}
