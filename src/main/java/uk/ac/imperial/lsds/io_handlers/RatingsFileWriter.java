package main.java.uk.ac.imperial.lsds.io_handlers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

public class RatingsFileWriter{
	
	private String path;
	private boolean isHDFS = false;
	private static Logger logger = Logger.getLogger(RatingsFileWriter.class);
	
	public RatingsFileWriter(String path){
		this.path = path;
		if(path.startsWith("hdfs"))
			this.isHDFS= true;
		else
			this.isHDFS= false;
	}
	
	/**
	 * 
	 * @param ratings
	 */
	private void persistNormalFS(List<String> ratings){
		
		try {
			File file = new File(this.path+"/ratings.data");
 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for(String line : ratings)
				bw.write(line+"\n");
			
			bw.close();
 
			logger.info(" Persisting ratings file to '"+ path +"' -> Done");
 
		} catch (IOException e) {
			logger.error("Writing ratings File to normal FS failed! Path : "+ path);
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param ratings
	 */
	private void persistHDFS(List<String> ratings) {
		logger.debug("HDFSFileWritter init..........");
		Configuration config = new Configuration();
        config.set("fs.default.name", this.path);        
        config.set("hadoop.tmp.dir", "/tmp/hadoop");
		
		try {
			// adding local hadoop configuration
			/*config.addResource(new Path(
					"/usr/local/Cellar/hadoop/2.4.0/libexec/etc/hadoop/core-site.xml"));
			config.addResource(new Path(
					"/usr/local/Cellar/hadoop/2.4.0/libexec/etc/hadoop/hdfs-site.xml"));
			*/

			String filename = this.path + "/ratings.data"; // this path is HDFS path
			
			Path pt = new Path(filename);
			FileSystem fs = FileSystem.get(new Configuration(config));
			//Delete if exists!!
			if(fs.exists(pt))
				fs.delete(pt, false);
			//then write new file
			BufferedWriter br;
//			if (fs.exists(pt)) {
//				br = new BufferedWriter(new OutputStreamWriter(fs.append(pt)));
			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			for(String line : ratings)
				br.write(line+"\n");

			br.close();
		} catch (Exception e) {
			logger.error("Writing ratings File to HDFS failed! Path : "+ path);
			e.printStackTrace();
		}

	}

	public void persistRatingsFile(List<String> ratings){
		if(!isHDFS)
			persistNormalFS(ratings);
		else
			persistHDFS(ratings);
	}
	
}
