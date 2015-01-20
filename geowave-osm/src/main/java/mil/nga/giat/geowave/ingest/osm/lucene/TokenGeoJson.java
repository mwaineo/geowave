package mil.nga.giat.geowave.ingest.osm.lucene;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
import org.geotools.geojson.geom.GeometryJSON;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;

public class TokenGeoJson {

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, ParseException {
		// TODO Auto-generated method stub
			GeometryJSON gjson = new GeometryJSON();

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
		  	  
		  	  
		  		  scn.fetchColumnFamily(new Text("geom"));
		  	  
		  	Map<String, String> items = new HashMap<String, String>();  
		  	
		  	  
		  	  for (Entry<Key, Value> kvp : scn){
		  		  Geometry geom = wkbreader.read(kvp.getValue().get());
		  		  
		  		  String json = null;
		  		  double tolerance = 0.01d;
		  		  Boolean done = false;
		  		  while (!done){
		  			  Geometry geom2 = null;
		  			  try {
		  				  if (tolerance > 0.00000001d){
					  		  DouglasPeuckerSimplifier simp = new DouglasPeuckerSimplifier(geom);
					  		  simp.setDistanceTolerance(tolerance);
					  		  simp.setEnsureValid(true);
					  		  geom2 = simp.getResultGeometry();
		  				  } else {
		  					  geom2 = geom;
		  				  }
			  		  json = gjson.toString(geom2);
			  		  done = true;
		  			  } catch (Exception ex){
		  				  ex.printStackTrace();
		  				  tolerance = tolerance / 10.0d;
		  			  }
		  		  }
		  		  
		  		  String token = kvp.getKey().getRow().toString();
		  		  String dir ="x:/git/bootleaf/assets/data/" + token; 
		  		  File directory = new File(dir);
		  		  if (!directory.exists())
		  			  directory.mkdir();
		  		  
		  		  File out = new File(dir + "/" + token + ".geojson");
		  		  FileWriter fw = null;
		  		  BufferedWriter w = null;
		  		  try {
		  			  fw = new FileWriter(out);
		  			  w = new BufferedWriter(fw);
		  			  w.write(json);
		  			  w.flush();fw.flush();
		  		  } catch (Exception ex){
		  			  ex.printStackTrace();
		  		  } finally {
		  			  try {w.close();} catch (Exception ex2){};
		  			  try {fw.close();} catch (Exception ex2){};
		  		  }
		  		  
		  		  items.put(token, kvp.getKey().getColumnQualifier().toString());
		  		  
		  	}
		  	 
		  	FileWriter fw2 = null;
		  	BufferedWriter bw2 = null;
		  	try {
		  	fw2 = new FileWriter(new File("x:/git/bootleaf/assets/data/tokens.json"));
		  	 bw2 = new BufferedWriter(fw2);
		  	 bw2.write("[");
		  	for (Entry<String, String> kvp : items.entrySet()){
		  		bw2.write("{");
		  		//bw2.write(kvp.getKey() + "," + kvp.getValue() + "\r\n");
		  		bw2.write("\"name\":\"" + kvp.getKey() + "\",");
		  		bw2.write("\"value\":\"" + kvp.getValue() + "\",");
		  		bw2.write("\"tokens\":[\"" + kvp.getKey() + "\"]");
		  		bw2.write("},");
		  	}
		  	
		  	bw2.write("]");
		  	
		  	bw2.flush();fw2.flush();bw2.close();fw2.close();
		  	} catch (Exception ex) {
		  		ex.printStackTrace();
		  	}
		  	  
		  	bw.flush();
			  bw.close();
			  scn.close();
	}

}
