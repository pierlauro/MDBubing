package dev.pstux.mdbubing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.bson.Document;

import it.unimi.di.law.warc.processors.ParallelFilteredProcessorRunner.Processor;
import it.unimi.di.law.warc.records.AbstractWarcRecord;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.ByteArraySessionOutputBuffer;

public class BSONWarcProcessor implements Processor<Document> {
	public static final BSONWarcProcessor INSTANCE = new BSONWarcProcessor();
	public static final long MDB_DOCUMENT_SIZE_LIMIT = 16 * 1024 * 1024;
	public static final String ADDITIONAL_HEADERS_MDB_FIELD_NAME = "headers";
	public static final String PAYLOAD_MDB_FIELD_NAME = "payload";
	private BSONWarcProcessor() {}

	private static final ByteArrayOutputStream os = new ByteArrayOutputStream();
	private static final ByteArraySessionOutputBuffer buf = new ByteArraySessionOutputBuffer();

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
		if(r.getWarcContentLength() >= MDB_DOCUMENT_SIZE_LIMIT) {
			// TODO log error with record ID
			return null;
		}

		Document obj = new Document();
		for(Header h: r.getWarcHeaders().getAllHeaders()) {
			obj.append(h.getName(), h.getValue());
		}

		processAdditionalHeaders(r, obj);

		try {
			// TODO optimize
			r.write(os, buf);
			String s = os.toString();
			s = s.substring(s.length() - (int)r.getWarcContentLength());
			obj.append(PAYLOAD_MDB_FIELD_NAME, s);
		} catch (IOException e) {
			// TODO log error with record ID
		}

		return obj;
	}

	void processAdditionalHeaders(WarcRecord r, Document obj) {
		AbstractWarcRecord record = (AbstractWarcRecord)r;

		// There could be duplicate header names (e.g. multiple Set-Cookie)
		Map<String, Object> headers = new HashMap<String, Object>();

		// TODO optimize
		for(Header h: record.getAllHeaders()) {
			String name = h.getName();
			String value = h.getValue();

			Object current = headers.get(name);
			if(current == null) {
				headers.put(name, value);
			}else {
				if(current instanceof String) {
					ArrayList<String> list = new ArrayList<String>();
					list.add((String)current);
					list.add(value);
					headers.put(name, list);
					continue;
				}

				@SuppressWarnings("unchecked")
				ArrayList<String> list = (ArrayList<String>)current;
				list.add(value);
			}
		}

		obj.append(ADDITIONAL_HEADERS_MDB_FIELD_NAME, headers);
	}
}
