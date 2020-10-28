package dev.pstux.mdbubing;

import static dev.pstux.mdbubing.TestConstants.NUM_VALID_TEST_WARC_RECORDS;
import static dev.pstux.mdbubing.TestConstants.TEST_WARC_TO_MONGO_PROPERTIES_PATH;

import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnit4.class)
@PrepareForTest(MongoClients.class)
public class WarcToMongoTest {
	@Test
	/** Test that all valid WARC records are inserted in the collection */
	public void validRecordsAreInsertedInCollection() throws Exception{
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

		// Invoke main with expected command line syntax
		WarcToMongo.main(new String[] {"-P", WarcToMongoTest.class.getResource(TEST_WARC_TO_MONGO_PROPERTIES_PATH).getPath()});

		// Verify that valid records are inserted in the collection
		verify(mockCollection, times(NUM_VALID_TEST_WARC_RECORDS)).insertOne(Mockito.any(Document.class));
	}
}
