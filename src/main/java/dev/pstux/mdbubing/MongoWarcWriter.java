package dev.pstux.mdbubing;

import java.io.IOException;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import dev.pstux.mdbubing.WarcToMongo.W2MConfiguration;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.di.law.warc.records.WarcRecord;

public class MongoWarcWriter implements WarcWriter {
	private MongoCollection<Document> collection;

	public MongoWarcWriter(final String configFilePath) throws Exception {
		W2MConfiguration configuration = WarcToMongo.loadConfiguration(configFilePath);

		// Initialize MongoDB entities
		this.collection = WarcToMongo.initializeConnection(configuration);
	}

	@Override
	public void close() throws IOException {
		// After closing, any access to the writer will return NullPointerException
		collection = null;
	}

	@Override
	public void write(WarcRecord record) throws IOException, InterruptedException {
		WarcToMongo.insertRecordInCollection(collection, record);
	}

}
