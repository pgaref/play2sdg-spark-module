package main.java.uk.ac.imperial.lsds.play2sdg;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import main.java.uk.ac.imperial.lsds.cassandra.CassandraQueryController;
import main.java.uk.ac.imperial.lsds.io_handlers.UserFileParser;
import main.java.uk.ac.imperial.lsds.io_handlers.LastFMDataParser;
import main.java.uk.ac.imperial.lsds.models.PlayList;
import main.java.uk.ac.imperial.lsds.models.Track;
import main.java.uk.ac.imperial.lsds.models.User;


public class PrepareData {
	
	
	/*
	 * TODO: pass these Values as arguments 
	 */
	private static String dataset_path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_train";
	private static String userdata_path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/users.txt";
	
	private static Logger  logger = Logger.getLogger(PrepareData.class);
	private List<User> users;
	private List<Track> tracks;
	private List<PlayList> playlists;
	
	
	private List<User> prepareUsers(){	
		UserFileParser userParser = new UserFileParser(userdata_path);
		List<User> users = userParser.ParseUsers();
		logger.debug("Persisting User Data! ### ");
		for(User u : users){
			System.out.println("\n Read user: "+ u);
			CassandraQueryController.persist(u);
			System.out.println("Persisted ...");
		}
		return users;
	}
	
	private List<Track> prepareTracks(){
		LastFMDataParser parser = new LastFMDataParser(dataset_path);
		List<Track> tracks = parser.parseDataSet(true);
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
				plist.addRatingSong(t);
			}
			plists.add(plist);
			CassandraQueryController.persist(plist);
			logger.debug("Persisted PlayList: "+plist.toString() +" for user: " +u.getEmail()+" ### ");
			
			
		}
		logger.debug("Finished Generating random PlayList Data! ### ");
		return plists;
	}
	
	public PrepareData() {
		
		this.users = prepareUsers();

		this.tracks = prepareTracks();
		
		this.playlists = preparePlayLists(20);
		
	}
	
	public static void main(String[] args) {
		
		/*
		 * Arguments so far: 1)UserFile 2)TracksFile 3)Number of tracks in default Playlist
		 */
		logger.setLevel(Level.DEBUG);
		logger.debug("Initating Prepare  Data for paths: Data path: " + dataset_path + " - User path:  "+ userdata_path+" ### ");
		PrepareData p = new PrepareData();
		logger.debug("Finished Preparing  Data for paths: Data path: " + dataset_path + " - User path:  "+ userdata_path+" ### ");
	}
	
	

}
