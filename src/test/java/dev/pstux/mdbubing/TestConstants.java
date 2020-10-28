package dev.pstux.mdbubing;

class TestConstants {
	// WARC file used for testing (records from a mongodb homepage crawling)
	protected static final String TEST_WARC_RESOURCE_PATH = "/mongodb-homepage.warc";

	// Number of valid WARC records in the test file before an invalid record
	protected static final int NUM_VALID_TEST_WARC_RECORDS = 3;
	
	// WarcToMongo configuration sample
	protected static final String TEST_WARC_TO_MONGO_PROPERTIES_PATH = "/WarcToMongo-sample-configuration.properties";

}
