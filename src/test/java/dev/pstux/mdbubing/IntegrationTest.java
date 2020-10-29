package dev.pstux.mdbubing;

import static dev.pstux.mdbubing.TestCommon.Constants.NUM_VALID_TEST_WARC_RECORDS;
import static dev.pstux.mdbubing.TestCommon.Constants.TEST_WARC_TO_MONGO_PROPERTIES_PATH;

import com.mongodb.client.MongoCollection;
import dev.pstux.mdbubing.WarcToMongo.WarcToMongoConfiguration;
import java.io.FileInputStream;
import java.util.Properties;
import org.bson.Document;

/**
 * This test is expected to be executed on a real MongoDB instance configured as prescribed in
 * TEST_WARC_TO_MONGO_PROPERTIES_PATH
 */
public class IntegrationTest {
  public static void main(String[] args) throws Exception {
    final String testWarcPath =
        WarcToMongoTest.class.getResource(TEST_WARC_TO_MONGO_PROPERTIES_PATH).getPath();

    WarcToMongo.main(new String[] {"-P", testWarcPath});

    FileInputStream fis = new FileInputStream(testWarcPath);
    Properties configProperties = new Properties();
    configProperties.load(fis);

    MongoCollection<Document> coll =
        WarcToMongo.initializeConnection(new WarcToMongoConfiguration(configProperties));

    // Exit with error status if the test failed
    System.exit(coll.countDocuments() == NUM_VALID_TEST_WARC_RECORDS ? 0 : 1);
  }
}
