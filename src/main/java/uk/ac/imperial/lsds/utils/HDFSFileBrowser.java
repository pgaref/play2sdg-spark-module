package main.java.uk.ac.imperial.lsds.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import main.java.uk.ac.imperial.lsds.models.Track;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class HDFSFileBrowser {

    static final Logger logger = Logger.getLogger(HDFSFileBrowser.class);

    private static Configuration configuration = new Configuration();
    private static FileSystem fileSystem;
    private static List<Path> paths;
    private static String path;
    
    HDFSFileBrowser(String path){
    	HDFSFileBrowser.path = path;
        try {
			init();
			start();
		} catch (IOException e) {
			logger.error("HDFSFileBrowser exception: " + e);
		}
        
    }

    private void init() throws IOException {
    	logger.debug("HDFSFileBrowser init..........");
        configuration.set("fs.default.name", HDFSFileBrowser.path);        
        configuration.set("hadoop.tmp.dir", "/tmp/hadoop");
        fileSystem = FileSystem.get(configuration);
        paths = new ArrayList<Path>();
    }
    
    private void start() throws IOException {
        FileStatus[] status = fileSystem.listStatus(new Path(HDFSFileBrowser.path));
        browse(status);
    }

    private static void printLine(Path p) throws IOException{
    	BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(p)));
        String line;
        line=br.readLine();
        while (line != null){
            System.out.println("LineNo " + paths.size() + "-->"+line);
            line=br.readLine();
        }
        br.close();
    	
    }
    private static void browse(FileStatus[] status) throws FileNotFoundException, IOException {
    	
    	for (int i = 0; i < status.length; i++) {
            FileStatus fileStatus = status[i];
            System.out.println(fileStatus.getPath());
            if (fileStatus.isDir()) {
                FileStatus[] subStatus = fileSystem.listStatus(fileStatus.getPath());
                browse(subStatus);
                logger.debug("HDFS Dir: "+ fileStatus.getPath());
            } else {
            	logger.debug("HDFS File: "+ fileStatus.getPath());
            	if(fileStatus.getPath().getName().endsWith(".json")){
            		paths.add(fileStatus.getPath());
            		//printLine(fileStatus.getPath());
            	}
            }
        }
    }

   
//    public static void main(String[] args) throws Exception {
//    	logger.setLevel(Level.DEBUG);
//        logger.debug("Starting HDFS Client..........");
//
//        HDFSFileBrowser browser = new HDFSFileBrowser("hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_subset");
//        browser.init();
//        browser.start();
//        
//      //  FSDataInputStream in  = fileSystem.open(paths.get(0));
//        BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(paths.get(0))));
////        String line;
////        line=br.readLine();
////        while (line != null){
////            System.out.println(line);
////            line=br.readLine();
////        }
//        
//
//	        
//
//    }

	/**
	 * @return the fileSystem
	 */
	public static FileSystem getFileSystem() {
		return fileSystem;
	}

	/**
	 * @param fileSystem the fileSystem to set
	 */
	public static void setFileSystem(FileSystem fileSystem) {
		HDFSFileBrowser.fileSystem = fileSystem;
	}

	/**
	 * @return the path
	 */
	public static String getPath() {
		return path;
	}

	/**
	 * @param path the path to set
	 */
	public static void setPath(String path) {
		HDFSFileBrowser.path = path;
	}

	/**
	 * @return the paths
	 */
	public static List<Path> getPaths() {
		return paths;
	}

	/**
	 * @param paths the paths to set
	 */
	public static void setPaths(List<Path> paths) {
		HDFSFileBrowser.paths = paths;
	}
}
