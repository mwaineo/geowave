package mil.nga.giat.geowave.ingest.osm.lucene;

import java.io.IOException;
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
import org.apache.accumulo.core.client.TableExistsException;
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

public class TokenCombiner {
	public static void main(String[] args) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException, ParseException {
		
		
		  
	  	  String zoo = "master.:2181";
	  	  String instance = "geowave";
	  	  String user = "root";
	  	  String pass = "geowave";
	  	  //String namespace = "osmhey";
	  	  //String table = "osm_virginia";
	  	  String nGramName = "osmngram";
	  	  String summaryName = "tokensummary";
	  	  
	  	  WKBWriter wkbwriter = new WKBWriter();
	  	  WKBReader wkbreader = new WKBReader();
	  	  
	  	  Text cftext = new Text("geom");
	  	  Text cqtext = new Text("wkb");
	  	  
	  	  System.out.println("combiner");
	  	  
	  	  
	  	  
	  	  ZooKeeperInstance inst = new ZooKeeperInstance("geowave", "master.:2181");
	  	  Connector conn = inst.getConnector("root", new PasswordToken("geowave"));
	  	  
	  	  BatchWriter bw = conn.createBatchWriter(summaryName, new BatchWriterConfig());
	  	  Scanner scn = conn.createScanner(nGramName,new Authorizations());
	  	  
	  	  scn.fetchColumnFamily(cftext);
	  	  
	  	  
	  	  List<Geometry> gc = new ArrayList<Geometry>();
	  	  String lastkey = "";
	  	  
	  	  int howmanyiputted = 0;
	  	  int numfacets = 0;
	  	  long entriesread = 0;
	  	  
	  	  for (Entry<Key, Value> kvp : scn){
	  		  entriesread++;
	  		  if (lastkey.equals("")){
	  			  lastkey = kvp.getKey().getRow().toString().trim();
	  			  //lastkey = kvp.getKey().toStringNoTime();
	  		  }
	  		  
	  		  if (gc.size() > 50000){
	  			  try {
		  			GeometryCollection gcol = GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection(gc.toArray(new Geometry[gc.size()]));
		  			Geometry union = gcol.union();
		  			union = DouglasPeuckerSimplifier.simplify(union, 0.001d);
		  			gc.clear();
		  			gc.add(union);
		  			System.out.println("condensing");
	  			  }
	  			  catch (java.lang.OutOfMemoryError | Exception ex2){
	  				  System.out.println("boom");
	  				  gc.clear();
	  			  }
	  		  }
	  		  
	  		  if (!kvp.getKey().getRow().toString().trim().equals(lastkey)){
	  		//if (!kvp.getKey().toStringNoTime().equals(lastkey)){
	  			  GeometryCollection gcol = GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection(gc.toArray(new Geometry[gc.size()]));
	  			  try {
		  			  Geometry union = gcol.union();
		  			  Mutation m = new Mutation(lastkey);
		  			  m.put(cftext, new Text(String.valueOf(numfacets)), new Value(wkbwriter.write(union)));
		  			  bw.addMutation(m);
		  			  lastkey = kvp.getKey().getRow().toString().trim();
		  			  gc.clear();
		  			  howmanyiputted++;
		  			  System.out.println(String.format("Unique: %d  || %.2f",  howmanyiputted, (float)(entriesread / 35000000f * 100f)));
		  			  numfacets = 0;
	  			  }
	  			  catch (Exception ex){
	  				lastkey = kvp.getKey().getRow().toString().trim();
		  			gc.clear();
		  			howmanyiputted++;
	  			  }
	  		  }
	  		  
	  		  Geometry geom =wkbreader.read(kvp.getValue().get());
	  		  if (geom.isValid()){
	  			  gc.add(geom);
	  			  numfacets++;
	  		  } else {
	  			  System.out.println("invalid");
	  		  }
	  	  }
	  	  
	  	System.out.println("combiner done");
	  	  
	}
}
