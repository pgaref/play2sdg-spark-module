package main.java.uk.ac.imperial.lsds.play2sdg;


import org.apache.log4j.Logger;

import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

import main.java.uk.ac.imperial.lsds.dx_controller.CassandraDxQueryController;
import main.java.uk.ac.imperial.lsds.dx_models.PlayList;
import main.java.uk.ac.imperial.lsds.dx_models.Track;
import main.java.uk.ac.imperial.lsds.dx_models.User;
import main.java.uk.ac.imperial.lsds.io_handlers.UserFileParser;
import main.java.uk.ac.imperial.lsds.io_handlers.LastFMDataParser;


public class PrepareData {
	
	
	/*
	 * TODO: pass these Values as arguments 
	 */
//	private static String dataset_path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_train";
//	private static String userdata_path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/users.txt";
	
	private static String dataset_path = "data/LastFM/lastfm_subset";
	private static String userdata_path = "data/users.txt";
	private CassandraDxQueryController queryController;
	
	private static Logger  logger = Logger.getLogger(PrepareData.class);
	private List<User> users;
	private List<Track> tracks;
	private List<PlayList> playlists;
	
	
	public PrepareData(Session clusterSession) {
		queryController = new CassandraDxQueryController(clusterSession);
		users = this.prepareUsers();
		tracks = this.prepareTracks();
		playlists = this.preparePlayLists(20);
	}
	
	public PrepareData(String userpath, String datapath, Session clusterSession) {
		queryController = new CassandraDxQueryController(clusterSession);
		dataset_path = datapath;
		userdata_path = userpath;
		users = this.prepareUsers();
		tracks = this.prepareTracks();
		playlists = this.preparePlayLists(20);
	}
	
	private List<User> prepareUsers(){	
		UserFileParser userParser = new UserFileParser(userdata_path);
		List<User> users = userParser.ParseUsers();
		logger.debug("Persisting User Data! ### ");
		for(User u : users){
			logger.debug("\n Read user: "+ u);
			queryController.persist(u);
			logger.debug("Persisted ...");
		}
		return users;
	}
	
	private List<Track> prepareTracks(){
		LastFMDataParser parser = new LastFMDataParser(dataset_path);
		List<Track> tracks = parser.parseDataSet();
		for(Track t : tracks)
			queryController.persist(t);
		logger.debug("Sucessfully persisted #"+ tracks.size() + "# Tracks" );
		return tracks;
		
	}
	
	private List<PlayList> preparePlayLists(int NumOfTracks){
		List<PlayList> plists = new ArrayList<PlayList>();
		logger.debug("Generating random PlayList Data! ### ");
		for(User u : this.users){
			PlayList plist = new PlayList(u.getEmail(), "EveyDayMusic");
			
			for(int i = 0 ; i < NumOfTracks ; i ++ ){
				Track t = this.tracks.get( (int)(Math.random()* this.tracks.size()) );
				plist.addRatingSong(t.getTitle());
			}
			plists.add(plist);
			queryController.persist(plist);
			logger.debug("Persisted PlayList: "+plist.toString() +" for user: " +u.getEmail()+" ### ");
			
			
		}
		logger.debug("Finished Generating random PlayList Data! ### ");
		return plists;
	}
	

//	public static void main(String[] args) {
//		
//		/*
//		 * Configurable Arguments so far: 1)UserFile 2)TracksFile 3)Number of tracks in default Playlist
//		 */
//		logger.setLevel(Level.DEBUG);
//		logger.debug("Initating Prepare  Data for paths: Data path: " + dataset_path + " - User path:  "+ userdata_path+" ### ");
//		PrepareData p = new PrepareData();
//		logger.debug("Finished Preparing  Data for paths: Data path: " + dataset_path + " - User path:  "+ userdata_path+" ### ");
//	}
	
	

}
