package mil.nga.giat.geowave.ingest.osm.lucene;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.store.GeometryUtils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;

public class TokenSimplifier {

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, ParseException, TableNotFoundException {

	  	  String zoo = "master.:2181";
	  	  String instance = "geowave";
	  	  String user = "root";
	  	  String pass = "geowave";
	  	  //String namespace = "osmhey";
	  	  //String table = "osm_virginia";
	  	  //String nGramName = "osmngram";
	  	  String summaryName = "tokensummary";
	  	  
	  	  WKBWriter wkbwriter = new WKBWriter();
	  	  WKBReader wkbreader = new WKBReader();
	  	  
	  	  Text cftext = new Text("geom");
	  	  Text cqtext = new Text("wkb");
	  	  
	  	  System.out.println("simplifier");
	  	  
	  	  
	  	  
	  	  ZooKeeperInstance inst = new ZooKeeperInstance("geowave", "master.:2181");
	  	  Connector conn = inst.getConnector("root", new PasswordToken("geowave"));
	  	  
	  	  BatchWriter bw = conn.createBatchWriter(summaryName, new BatchWriterConfig());
	  	  Scanner scn = conn.createScanner(summaryName,new Authorizations());
	  	  
	  	  scn.fetchColumnFamily(cftext);
	  	  
	  	  
	  	
	  	  
	  	  for (Entry<Key, Value> kvp : scn){
	  		  Geometry geom = wkbreader.read(kvp.getValue().get());
	  		  System.out.println(kvp.getKey().getRow());
	  		  double maxTolerance = 1;  
	  		  double minTolerance = 0.00001;
	  		  
	  		  for (int x = 1; x <= 18; x++){
	  			  Mutation m = new Mutation(kvp.getKey().getRow());  
	  			  double tolerance = 1 - (((maxTolerance - minTolerance) / 18 ) * (x - 1));
	  			  try {
		  			  Geometry geom2 = TopologyPreservingSimplifier.simplify(geom,tolerance);
		  			  m.put(new Text(String.valueOf(x)), kvp.getKey().getColumnQualifier() , new Value(wkbwriter.write(geom2)));
		  			  bw.addMutation(m);
	  			  } catch (Exception ex) {
	  				  System.out.println(ex.getMessage());
	  			  }
	  		  }
	  		  
	  		  
	  		  
	  	}
	  	bw.flush();
		  bw.close();
		  scn.close();
	}

}
