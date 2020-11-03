package dev.pstux.mdbubing;

import com.mongodb.client.MongoCollection;
import dev.pstux.mdbubing.WarcToMongo.WarcToMongoConfiguration;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.di.law.warc.records.WarcRecord;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoWarcWriter implements WarcWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoWarcWriter.class);
  private static AtomicInteger nextWriterID = new AtomicInteger(0);

  private MongoCollection<Document> collection;
  private int id = nextWriterID.getAndIncrement();

  public MongoWarcWriter(final String configFilePath) throws Exception {
    this(WarcToMongo.loadConfiguration(configFilePath));
  }

  public MongoWarcWriter(final WarcToMongoConfiguration configuration) throws Exception {
    this.collection = WarcToMongo.initializeConnection(configuration);
    LOGGER.info("Initializing {} with ID {}", MongoWarcWriter.class.getName(), id);
  }

  public int getId() {
    return id;
  }

  @Override
  public void close() throws IOException {
    // After closing, any access to the writer will result in a NullPointerException
    collection = null;
    LOGGER.info("Closing {} with ID {}", MongoWarcWriter.class.getName(), id);
  }

  @Override
  public void write(WarcRecord record) throws IOException, InterruptedException {
    WarcToMongo.insertRecordInCollection(collection, record);
  }
}
