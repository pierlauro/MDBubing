package dev.pstux.mdbubing;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

class TestCommon {
  class Constants {
    // WARC file used for testing (records from a mongodb homepage crawling)
    protected static final String TEST_WARC_RESOURCE_PATH = "/mongodb-homepage.warc";

    // Number of valid WARC records in the test file before an invalid record
    protected static final int NUM_VALID_TEST_WARC_RECORDS = 3;

    // WarcToMongo configuration sample
    protected static final String TEST_WARC_TO_MONGO_PROPERTIES_PATH =
        "/WarcToMongo-sample-configuration.properties";
  }

  /**
   * Mock connection, database and collection creation and return a mock collection.
   *
   * <p>Tests using this method should be executed by the PowerMockRunner and should prepare
   * MongoClients for
   * test @RunWith(PowerMockRunner.class) @PowerMockRunnerDelegate(JUnit4.class) @PrepareForTest(MongoClients.class)
   */
  static MongoCollection<Document> mockMongoCollectionCreation() {
    // Mock client, database and collection objects
    @SuppressWarnings("unchecked")
    MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
    MongoDatabase mockDatabase = Mockito.mock(MongoDatabase.class);
    Mockito.doReturn(mockCollection).when(mockDatabase).getCollection(Mockito.anyString());
    MongoClient mockClient = Mockito.mock(MongoClient.class);
    Mockito.doReturn(mockDatabase).when(mockClient).getDatabase(Mockito.anyString());

    // Mock client creator
    PowerMockito.mockStatic(MongoClients.class);
    BDDMockito.given(MongoClients.create(Mockito.anyString())).willReturn(mockClient);

    return mockCollection;
  }
}
