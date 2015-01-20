package mil.nga.giat.geowave.ingest.osm;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader;


public class OSMPBFStage {

	
	private final Logger log = Logger.getLogger(OSMPBFStage.class);

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://master.:8020");
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		OSMPBFStage stg = new OSMPBFStage();
		long time = System.currentTimeMillis();
		stg.stageToHadoop("d:\\virginia-latest.osm.pbf", "/osm/stage2", conf);
		System.out.println((System.currentTimeMillis() - time) / 1000.0f);
	}
	
	private void stageToHadoop(String localPBFInput, String sequenceFilePath, Configuration conf) {
		Path path = new Path(sequenceFilePath);
		LongWritable key = new LongWritable();
		BytesWritable value = new BytesWritable();
		SequenceFile.Writer writer = null;
		
		
		DataInputStream in = null;
		InputStream is = null;
	    try {  
	    	//File spec @ http://wiki.openstreetmap.org/wiki/PBF_Format
	    	File localPBF = new File(localPBFInput);
	    	is = new FileInputStream(localPBF);
	    	in = new DataInputStream(is);
	    	writer = SequenceFile.createWriter(conf,  SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()));
	    	long blockid = 0;
	    	while (in.available() > 0){
		        if (in.available() == 0) {break;}
			    int len = in.readInt();
			    
			
			    byte[] blobHeader = new byte[len]; 
			    in.read(blobHeader);
			    BlobHeader h = BlobHeader.parseFrom(blobHeader);
			    
			    if (h.getType().equals("OSMData")){
			    	byte[] blob = new byte[h.getDatasize()];
			    	in.read(blob,0, h.getDatasize());
			    	key.set(blockid);
				    value.set(blob,0, blob.length);
				    writer.append(key, value);
				    blockid++;
			    }
			    else {
			    	in.skip(h.getDatasize());
			    }
			    
			    
			    
		    }
	   } catch (IOException e) {
		   System.out.println(e.getLocalizedMessage());
		   log.error(e.getLocalizedMessage());
		} finally {
			if (is != null) {try {is.close();}catch(Exception e){}};
			if (in != null) {try {in.close();}catch(Exception e){}};
			if (writer != null) {try {writer.close();} catch(Exception e){}};
		}
	}
		
	


}
