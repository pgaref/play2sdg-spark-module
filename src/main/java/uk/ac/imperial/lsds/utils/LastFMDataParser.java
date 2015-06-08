package main.java.uk.ac.imperial.lsds.utils;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import main.java.uk.ac.imperial.lsds.models.Track;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

/* 	LINK: http://labrosa.ee.columbia.edu/millionsong/lastfm#getting
 *  943,347 matched tracks MSD <-> Last.fm
 *  505,216 tracks with at least one tag
 *	584,897 tracks with at least one similar track
 *	522,366 unique tags
 *	8,598,630 (track - tag) pairs
 *	56,506,688 (track - similar track) pairs
 */
public class LastFMDataSet {

	private String path;
	private File file = null;
	private String[] subdirs;
	private List<File> allFiles;
	
	private static Logger  logger = Logger.getLogger("main.java.uk.ac.imperial.lsds.utils.LastFMDataSet");
	

	public LastFMDataSet(String path) {
		this.path = path;
		file = new File(path);
		this.subdirs = this.getSubDirectories();
		this.allFiles = listAllFiles(path);
		
	}
	

	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}

	/**
	 * @return the subdirs
	 */
	public String[] getSubdirs() {
		return subdirs;
	}

	/**
	 * @param path the path to set
	 */
	public void setPath(String path) {
		this.path = path;
	}

	/**
	 * @param subdirs the subdirs to set
	 */
	public void setSubdirs(String[] subdirs) {
		this.subdirs = subdirs;
	}

	public String[] getSubDirectories() {

		return file.list(new FilenameFilter() {
			@Override
			public boolean accept(File current, String name) {
				return new File(current, name).isDirectory();
			}
		});
	}
	
	public static List<File> listAllFiles(String directoryName) {
        File directory = new File(directoryName);
        List<File> resultList = new ArrayList<File>();
        // get all the files from a directory
        File[] fList = directory.listFiles();
        //resultList.addAll(Arrays.asList(fList));
        for (File file : fList) {
            if (file.isFile()) {
                logger.debug("Adding File: "+ file.getAbsolutePath());
                //Avoid hidden and system files!
                if(file.getName().contains(".json"))
                	resultList.add(file);
            } else if (file.isDirectory()) {
            	logger.debug("Ignoring dir: " + file);
                resultList.addAll(listAllFiles(file.getAbsolutePath()));
            }
        }      
        return resultList;
    }
	
	public static void checkTrackJsonFields(JSONObject trackjson){
		/*
		 * Check all the mandatory fields we need are not null!
		 */
		assert( trackjson.get("track_id") != null);
		assert( trackjson.get("artist") != null);
		assert( trackjson.get("title") != null);
		assert( trackjson.get("timestamp") != null);
		
	}
	
	public static void dumpTrack(JSONObject trackjson){
		logger.debug("Creating Track "+ trackjson.get("track_id"));
		Track t  = new Track((String)trackjson.get("track_id"), (String)trackjson.get("title"), (String)trackjson.get("artist"), (String)trackjson.get("timestamp"));
		logger.debug("Sucessfuly created "+ trackjson.get("track_id"));
		CassandraController.persist(t);
		logger.debug("Sucessfuly persisted "+ trackjson.get("track_id") + " to Cassandra");
	}
	

		  
	public static List<Track> JsonReadTracksFromFile(File f, boolean persist){
        
		JSONParser parser = new JSONParser();
    	
		ContainerFactory containerFactory = new ContainerFactory() {
    		public List creatArrayContainer() {
    			return new LinkedList();
    		}

    		public Map createObjectContainer() {
    			return new LinkedHashMap();
    		}

    	};
    	/*
    	 * Convert json files to Tracks!
    	 */
    	List<Track> spotifytracks = new ArrayList<Track>();
    	
        try {
 
			Object obj = parser.parse(new FileReader(f));

			JSONObject jsonObject = (JSONObject) obj;
			LastFMDataSet.checkTrackJsonFields(jsonObject);
			

			Map json = (Map) parser.parse(jsonObject.toJSONString(), containerFactory);
			Iterator iter = json.entrySet().iterator();
			System.out.println("==Creating new Track: " +jsonObject.get("track_id") +  "==");
			if(persist)
				LastFMDataSet.dumpTrack(jsonObject);
			
//			while (iter.hasNext()) {
//				Map.Entry entry = (Map.Entry) iter.next();
//				System.out.println(entry.getKey() + "=>" + entry.getValue());
//			}
                                    
			
 
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return spotifytracks;
	}

	
	public static void main(String[] args) {
		logger.setLevel(Level.DEBUG);
		LastFMDataSet dataset = new LastFMDataSet("data/LastFM/lastfm_subset");
		//LastFMDataSet dataset = new LastFMDataSet("data/LastFM/lastfm_train");
		
		for(File f : dataset.allFiles){
			System.out.println("## File: " + f);
			JsonReadTracksFromFile(f, true);
		}
		
		System.out.println("Tracks Num ## "+ dataset.allFiles.size());
		logger.info("Sucessfully dumped #"+ dataset.allFiles.size() + "# Tracks" );
	}

}
