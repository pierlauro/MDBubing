package dev.pstux.mdbubing;

import static dev.pstux.mdbubing.TestCommon.Constants.TEST_WARC_RESOURCE_PATH;
import static dev.pstux.mdbubing.TestCommon.Constants.TEST_WARC_TO_MONGO_PROPERTIES_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import it.unimi.di.law.warc.io.UncompressedWarcReader;
import it.unimi.di.law.warc.io.WarcReader;
import it.unimi.di.law.warc.records.WarcRecord;
import java.lang.reflect.Field;
import java.net.URL;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnit4.class)
@PrepareForTest(MongoClients.class)
public class MongoWarcWriterTest {
  private final String testConfFilePath =
      MongoWarcWriterTest.class.getResource(TEST_WARC_TO_MONGO_PROPERTIES_PATH).getPath();
  private MongoCollection<Document> mockCollection;
  private MongoWarcWriter writer;

  @Before
  public void setUp() throws Exception {
    // Initialize collection that will receive the writes
    mockCollection = TestCommon.mockMongoCollectionCreation();

    // Initialize the  writer
    writer = new MongoWarcWriter(testConfFilePath);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInitAndClose() throws Exception {
    Field collectionField = writer.getClass().getDeclaredField("collection");
    collectionField.setAccessible(true);
    assertEquals((MongoCollection<Document>) collectionField.get(writer), mockCollection);
    writer.close();
    assertNull(collectionField.get(writer));
  }

  @Test
  public void testWrite() throws Exception {
    // Read from test WARC file
    URL url = BSONWarcProcessorTest.class.getResource(TEST_WARC_RESOURCE_PATH);
    WarcReader reader = new UncompressedWarcReader(url.openStream());
    WarcRecord record = reader.read();

    writer.write(record);

    // Verify that passing a valid records results in a document inserted in the collection
    verify(mockCollection, times(1)).insertOne(Mockito.any(Document.class));

    writer.close();
  }
}
