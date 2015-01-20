package mil.nga.giat.geowave.ingest.osm.lucene;

import java.io.Reader;












import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

public class OSMAnalyzer extends StopwordAnalyzerBase {

	public int maxTokenLength = 20;
		
	protected OSMAnalyzer() {
		super(Version.LUCENE_41, StandardAnalyzer.STOP_WORDS_SET);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected TokenStreamComponents createComponents(String field, Reader reader) {
		 	final Tokenizer source = new LetterTokenizer(matchVersion, reader);
		 	
	        //source.setMaxTokenLength(maxTokenLength);

	        TokenStream pipeline = source;
	        pipeline = new StandardFilter(matchVersion, pipeline);
	        pipeline = new EnglishPossessiveFilter(matchVersion, pipeline);
	        pipeline = new ASCIIFoldingFilter(pipeline);
	        pipeline = new LowerCaseFilter(matchVersion, pipeline);
	        pipeline = new StopFilter(matchVersion, pipeline, stopwords);
	        pipeline = new PorterStemFilter(pipeline);
	        return new TokenStreamComponents(source, pipeline);
	}

}
