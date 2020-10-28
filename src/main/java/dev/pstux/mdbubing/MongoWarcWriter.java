package dev.pstux.mdbubing;

import java.io.IOException;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import dev.pstux.mdbubing.WarcToMongo.WarcToMongoConfiguration;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.di.law.warc.records.WarcRecord;

public class MongoWarcWriter implements WarcWriter {
	private MongoCollection<Document> collection;

	public MongoWarcWriter(final String configFilePath) throws Exception {
		this(WarcToMongo.loadConfiguration(configFilePath));
	}

	public MongoWarcWriter(final WarcToMongoConfiguration configuration) throws Exception {
		this.collection = WarcToMongo.initializeConnection(configuration);
	}

	@Override
	public void close() throws IOException {
		// After closing, any access to the writer will result in a NullPointerException
		collection = null;
	}

	@Override
	public void write(WarcRecord record) throws IOException, InterruptedException {
		WarcToMongo.insertRecordInCollection(collection, record);
	}
}
