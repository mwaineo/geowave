package mil.nga.giat.geowave.ingest.osm.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.ingest.accumulo.OSMConversionIterator;
import mil.nga.giat.geowave.ingest.osm.OSMFeatureBuilder;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

public class TokenizerTest {
	
	private static final Analyzer _analyzer = new OSMAnalyzer();

	public static void main(String[] args) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
		
		
		  
  	  String zoo = "master.:2181";
  	  String instance = "geowave";
  	  String user = "root";
  	  String pass = "geowave";
  	  String namespace = "osmhey";
  	  String table = "osm_virginia";
  	  String nGramName = "osmngram";
  	  
  	  WKBWriter wkbwriter = new WKBWriter();
  	  
  	  Text cftext = new Text("geom");
  	  Text cqtext = new Text("wkb");
  	  
  	  
  	  
  	  
  	  ZooKeeperInstance inst = new ZooKeeperInstance("geowave", "master.:2181");
  	  Connector conn = inst.getConnector("root", new PasswordToken("geowave"));
  	  
  	  
  	  BatchWriterConfig bwc = new BatchWriterConfig();
  	  bwc.setMaxLatency(30, TimeUnit.SECONDS);
  	  bwc.setTimeout(45, TimeUnit.SECONDS);
  	  BatchWriter bw = conn.createBatchWriter(nGramName, bwc);
  	  
  	   
      if (!conn.tableOperations().exists(nGramName)){
      	conn.tableOperations().create(nGramName);
      }
      
  	  
  	   //ClientSideIteratorScanner csis = new ClientSideIteratorScanner(conn.createScanner("osm_virginia", new Authorizations()));
 	   Scanner csis = conn.createScanner(table, new Authorizations());
 	  //csis.setRanges(OSMFeatureBuilder.getRanges());
 	   IteratorSetting si = new IteratorSetting(50, "filter", OSMConversionIterator.class);
 	   String qualifier = "w";
 	   si.addOption("cq", "waytags");
 	   csis.addScanIterator(si);
 	   for (Entry<Key, Value> entry :  csis){
 		  Map<String, String> info = new HashMap<String, String>();
 		  Map<String, String> main = new HashMap<String, String>();
 		  Map<String, String> tags = new HashMap<String, String>();
 		  
 		  for (Entry<Key,Value> entry2 : OSMConversionIterator.decodeRow(entry.getKey(),entry.getValue()).entrySet()){
 			  String cf = entry2.getKey().getColumnFamily().toString();
 			  String cq = entry2.getKey().getColumnQualifier().toString();
 			  String v = new String(entry2.getValue().get());
 			  if (cf.contains("tags")){
 				  tags.put(cq, v);
 			  } else if (cf.contains("info")) {
 				  info.put(cq, v);
 			  } else {
 				  main.put(cq, v);
 			  }
 		  }
 		 Geometry geom =	OSMFeatureBuilder.getGeometry(conn, table, qualifier, tags, main, info, null);
 		 if (geom != null){
 			 
 			 StringBuilder sb = new StringBuilder();
 			 for (Entry<String, String> kvp : info.entrySet()){
 				sb.append(kvp.getKey());
 				sb.append(" ");
 				sb.append(kvp.getValue());
 				sb.append(" ");
 			 }
 			for (Entry<String, String> kvp : main.entrySet()){
 				sb.append(kvp.getKey());
 				sb.append(" ");
 				sb.append(kvp.getValue());
 				sb.append(" ");
 			 }
 			for (Entry<String, String> kvp : tags.entrySet()){
 				sb.append(kvp.getKey());
 				sb.append(" ");
 				sb.append(kvp.getValue());
 				sb.append(" ");
 			 }
 			
 			List<String> tokens = tokenizeString(sb.toString());
 			for (String s : tokens){
 				Mutation m = new Mutation(s);
 				m.put(cftext, cqtext, new Value(wkbwriter.write(geom)));
 				bw.addMutation(m);
 			}
 			
 			System.out.println(tokens.size());
 		 }
 	  }
 	  csis.close();
 	  
 	  System.out.println("done");
	}
	
	public static List<String> tokenizeString(String string) {
	    List<String> result = new ArrayList<String>();
	    try {
	      TokenStream stream  = _analyzer.tokenStream(null, new StringReader(string));
	      stream.reset();
	      while (stream.incrementToken()) {
	        result.add(stream.getAttribute(CharTermAttribute.class).toString());
	      }
	    } catch (IOException e) {
	      // not thrown b/c we're using a string reader...
	      throw new RuntimeException(e);
	    }
	    return result;
	  }

}
