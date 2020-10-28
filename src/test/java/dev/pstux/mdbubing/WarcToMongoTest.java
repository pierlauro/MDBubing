package dev.pstux.mdbubing;

import static dev.pstux.mdbubing.TestCommon.Constants.NUM_VALID_TEST_WARC_RECORDS;
import static dev.pstux.mdbubing.TestCommon.Constants.TEST_WARC_TO_MONGO_PROPERTIES_PATH;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnit4.class)
@PrepareForTest(MongoClients.class)
public class WarcToMongoTest {
	@Test
	/** Test that all valid WARC records are inserted in the collection */
	public void validRecordsAreInsertedInCollection() throws Exception{
		MongoCollection<Document> mockCollection = TestCommon.mockMongoCollectionCreation();

		// Invoke main with expected command line syntax
		WarcToMongo.main(new String[] {"-P", WarcToMongoTest.class.getResource(TEST_WARC_TO_MONGO_PROPERTIES_PATH).getPath()});

		// Verify that valid records are inserted in the collection
		verify(mockCollection, times(NUM_VALID_TEST_WARC_RECORDS)).insertOne(Mockito.any(Document.class));
	}
}
