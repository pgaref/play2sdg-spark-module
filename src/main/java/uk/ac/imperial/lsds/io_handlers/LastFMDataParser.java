package main.java.uk.ac.imperial.lsds.io_handlers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import main.java.uk.ac.imperial.lsds.dx_controller.CassandraDxQueryController;
import main.java.uk.ac.imperial.lsds.dx_models.Track;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/* 	LINK: http://labrosa.ee.columbia.edu/millionsong/lastfm#getting
 *  943,347 matched tracks MSD <-> Last.fm
 *  505,216 tracks with at least one tag
 *	584,897 tracks with at least one similar track
 *	522,366 unique tags
 *	8,598,630 (track - tag) pairs
 *	56,506,688 (track - similar track) pairs
 */
public class LastFMDataParser {

	private static String path;
	private static File file = null;
	private static String[] subdirs;
	private static List<File> allFiles;
	private static boolean isHDFS;
	private static List<Track> spotifytracks = new ArrayList<Track>();
	HDFSFileBrowser browser = null;
	
	private static Logger  logger = Logger.getLogger(LastFMDataParser.class);

	public LastFMDataParser(String path) {
		
		
		LastFMDataParser.path = path;
		if(path.startsWith("hdfs"))
			LastFMDataParser.isHDFS= true;
		else
			LastFMDataParser.isHDFS= false;
		
		if(isHDFS){
	        	browser = new HDFSFileBrowser(path);
		}
		else{
			LastFMDataParser.file = new File(path);
			LastFMDataParser.subdirs = this.getSubDirectories();
			LastFMDataParser.allFiles = listAllFiles(path);
		}

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
                if(file.getName().endsWith(".json"))
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
		assert( trackjson.get("tags") != null);
		
	}
	
	public static Track dumpTrack(JSONObject trackjson){
		logger.debug("Creating Track "+ trackjson.get("track_id"));
		Track t  = new Track((String)trackjson.get("track_id"), (String)trackjson.get("title"), (String)trackjson.get("artist"), (String)trackjson.get("timestamp"));
		JSONArray tags = (JSONArray)  trackjson.get("tags");
		JSONArray similars = (JSONArray)  trackjson.get("similars");
		for(Object tag : tags)
			t.getTags().add(tag.toString());
		for(Object similar : similars)
			t.getSimilars().add(similar.toString());	
		logger.debug("Sucessfuly created "+ trackjson.get("track_id"));
//		if(perist){
//			CassandraQueryController.persist(t);
//			logger.debug("Sucessfuly persisted "+ trackjson.get("track_id") + " to Cassandra");
//		}
		return t;
	}

	public static void JsonPersistTracksFromFile(File f,CassandraDxQueryController queryController) {

		JSONParser parser = new JSONParser();

		ContainerFactory containerFactory = new ContainerFactory() {
			public List creatArrayContainer() {
				return new LinkedList();
			}
			public Map createObjectContainer() {
				return new LinkedHashMap();
			}
		};
		try {
			Object obj = parser.parse(new FileReader(f));
			JSONObject jsonObject = (JSONObject) obj;
			LastFMDataParser.checkTrackJsonFields(jsonObject);

			Map json = (Map) parser.parse(jsonObject.toJSONString(),
					containerFactory);
			// Iterator iter = json.entrySet().iterator();
			logger.debug("== Creating new Track: " + jsonObject.get("track_id")
					+ " ==");
			// Add track to List and Optionally save track to Cassandra
			// Back-end!
			queryController.persist(LastFMDataParser.dumpTrack(jsonObject));
			logger.debug(" ---> Successfully Persisted track");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		  
	public static void JsonReadTracksFromFile(File f){
        
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
        try {
 
			Object obj = parser.parse(new FileReader(f));
			JSONObject jsonObject = (JSONObject) obj;
			LastFMDataParser.checkTrackJsonFields(jsonObject);

			Map json = (Map) parser.parse(jsonObject.toJSONString(), containerFactory);
			//Iterator iter = json.entrySet().iterator();
			logger.debug("== Creating new Track: " +jsonObject.get("track_id") +  " ==");
			//Add track to List and Optionally save track to Cassandra Back-end!
			spotifytracks.add(LastFMDataParser.dumpTrack(jsonObject));
			logger.debug(" ---> TacksList size: "+spotifytracks.size() );
			
//			while (iter.hasNext()) {
//				Map.Entry entry = (Map.Entry) iter.next();
//				System.out.println(entry.getKey() + "=>" + entry.getValue());
//			}
                                     
        } catch (Exception e) {
            e.printStackTrace();
        }
        
	}
	
	public static void JsonReadTracksFromHDFS(Path p){
        
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
    	 * Convert json files from HDFS to Tracks!
    	 */
    	BufferedReader br = null;
        try {
 
        	br = new BufferedReader(new InputStreamReader(HDFSFileBrowser.getFileSystem().open(p)));
			Object obj = parser.parse(br);
			
			JSONObject jsonObject = (JSONObject) obj;
			LastFMDataParser.checkTrackJsonFields(jsonObject);

			Map json = (Map) parser.parse(jsonObject.toJSONString(), containerFactory);
			
			//Iterator iter = json.entrySet().iterator();
			logger.info("== Creating new Track: " +jsonObject.get("track_id") +  " ==");
			//Add track to List and Optionally save track to Cassandra Back-end!
			spotifytracks.add(LastFMDataParser.dumpTrack(jsonObject));
			logger.info(" ---> TacksList size: "+spotifytracks.size() );
            br.close();                         
        } catch (BlockMissingException e) {
            logger.error("HDFS Block Missing exception: " + e);
        } catch (ParseException e) {
        	 logger.error("JSON parse exception: " + e);
		} catch (IOException e) {
			logger.error("JSON IOexception: " + e);
		} finally{
			if(br != null){
				try {
					br.close();
				} catch (IOException e) {
					logger.error("Closing BufferedReader IOexception: " + e);
				}
			}
		}
	}
	
	public List<Track> parseDataSet(){
		if(!isHDFS){
			for(File f : allFiles){
				logger.debug("## fs File: " + f);
				JsonReadTracksFromFile(f);
			}
		}
		else{
			for(Path p : HDFSFileBrowser.getPaths()){
				logger.debug("## hdfs File: " + p.getName());
				JsonReadTracksFromHDFS(p);
			}
			
		}
		logger.info(" ---> TacksList TOTAL size: "+spotifytracks.size() );
		return spotifytracks;
	}

	public int persitDataSet(CassandraDxQueryController queryController){
		int count =0;
		if(!isHDFS){
			for(File f : allFiles){
				logger.debug("## fs File: " + f);
				JsonPersistTracksFromFile(f,queryController);
				count++;
			}
		}
//		else{
//			for(Path p : HDFSFileBrowser.getPaths()){
//				logger.debug("## hdfs File: " + p.getName());
//				JsonReadTracksFromHDFS(p);
//			}
//			
//		}
		logger.info(" ---> TacksList TOTAL size: "+spotifytracks.size() );
		return count;
		
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
		LastFMDataParser.path = path;
	}

	/**
	 * @param subdirs the subdirs to set
	 */
	public void setSubdirs(String[] subdirs) {
		LastFMDataParser.subdirs = subdirs;
	}

	/*
	 * #######################################
	 */
	static
	{
	    Logger rootLogger = Logger.getRootLogger();
	    rootLogger.setLevel(Level.INFO);
	    rootLogger.addAppender(new ConsoleAppender(
	               new PatternLayout("%-6r [%p] %c - %m%n")));
	}

	
	
	public static void main(String[] args) {
		
		logger.setLevel(Level.DEBUG);
		
		//LastFMDataParser parser = new LastFMDataParser( "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_train");
		LastFMDataParser parser = new LastFMDataParser( "data/LastFM/lastfm_subset");
		//LastFMDataParser parser = new LastFMDataParser( "data/LastFM/lastfm_train");
		
		
		List<Track> tracksList = parser.parseDataSet();
		logger.debug("Sucessfully dumped #"+ tracksList.size() + "# Tracks" );
		
//		for(Track t : tracksList ){
//			System.out.println ( " Track index: "+ tracksList.indexOf(t)  + " with Title "+ t.getTitle() );
//			break;
//		}
		
//		User u = CassandraQueryController.findbyEmail("pgaref@example.com");
//		PlayList plist = new PlayList("pgaref@example.com", "LoungeMusic");
//		plist.addRatingSong(tracksList.get(0));
//		CassandraQueryController.persist(plist);
//		
//		Recommendation rtest = new Recommendation("pgaref@example.com");
//		rtest.addRecommendation(tracksList.get(1).getTitle(), 2.0);
//		CassandraQueryController.persist(rtest);
		
	}
}
