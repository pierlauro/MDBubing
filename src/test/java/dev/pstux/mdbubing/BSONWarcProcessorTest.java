package dev.pstux.mdbubing;

import java.net.URL;

import org.junit.Test;

import it.unimi.di.law.warc.io.UncompressedWarcReader;
import it.unimi.di.law.warc.io.WarcReader;
import it.unimi.di.law.warc.records.WarcRecord;

public class BSONWarcProcessorTest {
	private final BSONWarcProcessor processor = BSONWarcProcessor.INSTANCE;
	
	@Test
	public void test() throws Exception	{
		URL url = BSONWarcProcessorTest.class.getResource("/sample.warc");
		WarcReader reader = new UncompressedWarcReader(url.openStream());
		WarcRecord record;
		while((record= reader.read()) != null) {
			processor.process(record, 0);
		}
	}
}
