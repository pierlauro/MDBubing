package dev.pstux.mdbubing;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.cli.MissingArgumentException;
import org.bson.Document;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;

import it.unimi.di.law.warc.io.UncompressedWarcReader;
import it.unimi.di.law.warc.io.WarcFormatException;
import it.unimi.di.law.warc.io.WarcReader;
import it.unimi.di.law.warc.records.WarcRecord;

public class WarcToMongo {
	public static class W2MConfiguration{
		private String connectionString;
		private String database;
		private String collection;
		private String warcFilePath;

		public W2MConfiguration(Properties props) throws Exception {
			for(Field f: this.getClass().getDeclaredFields()) {
				f.setAccessible(true);
				String name = f.getName();
				String value = props.getProperty(f.getName());
				if(value == null) {
					throw new MissingArgumentException("Parameter `" + name + "` missing in " + WarcToMongo.class.getName() + " configuration properties\n" + getRequiredParameters());
				}
				f.set(this, value);
			}
		}

		public static String getRequiredParameters() throws Exception{
			StringBuffer sb = new StringBuffer("Required configuration properties:\n");
			Arrays.stream(W2MConfiguration.class.getDeclaredFields()).forEach(f -> sb.append("\t- " + f.getName() + "\n"));
			return sb.toString();
		}
	}

	private static final String PROPERTIES_FILE_OPTION = "properties";

	public static void main(String[] args) throws Exception {
		// Parse command line options
		final SimpleJSAP jsap = new SimpleJSAP(
				WarcToMongo.class.getName(), "Starts a WarcToMongo...." /* TODO improve description */,
				new Parameter[] {
						new FlaggedOption(PROPERTIES_FILE_OPTION, JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'P', PROPERTIES_FILE_OPTION, "The properties used to configure WarcToMongo." + W2MConfiguration.getRequiredParameters()),
				}
				);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) {
			System.err.println("Usage: java " + WarcToMongo.class.getName() + " " + jsap.getUsage());
			System.exit(1);
		}

		// Load configuration
		W2MConfiguration configuration = loadConfiguration(jsapResult.getString(PROPERTIES_FILE_OPTION));

		// Initialize MongoDB entities
		MongoCollection<Document> coll = initializeConnection(configuration);

		// Initialize WARC reader on the provided file
		URL url = new File(configuration.warcFilePath).toURI().toURL();

		// TODO support also non-uncompressed WARC formats
		WarcReader reader = new UncompressedWarcReader(url.openStream());
		WarcRecord record = null;

		// Insert in the collection one document per valid WARC record
		while(true) {
			try {
				record = reader.read();
				if(record == null) {
					break;
				}
			}catch(WarcFormatException e) {
				// TODO log exception
				continue;
			}

			insertRecordInCollection(coll, record);
		}
		System.out.println(coll.countDocuments());
	}

	public static W2MConfiguration loadConfiguration(final String configFilePath) throws Exception {
		FileInputStream fis = new FileInputStream(configFilePath);
		Properties configProperties =  new Properties();
		configProperties.load(fis);
		return new W2MConfiguration(configProperties);
	}

	public static MongoCollection<Document> initializeConnection(final W2MConfiguration config){
		MongoClient mongoClient = MongoClients.create(config.connectionString);
		MongoDatabase database = mongoClient.getDatabase(config.database);
		return database.getCollection(config.collection);
	}

	public static InsertOneResult insertRecordInCollection(MongoCollection<Document> coll, WarcRecord record) {
		Document d = BSONWarcProcessor.INSTANCE.process(record, 0);
		if(d != null) {
			return coll.insertOne(d);
		}
		return null;
	}
}
