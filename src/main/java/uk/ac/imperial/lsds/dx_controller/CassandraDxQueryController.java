package main.java.uk.ac.imperial.lsds.dx_controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import main.java.uk.ac.imperial.lsds.dx_models.Counter;
import main.java.uk.ac.imperial.lsds.dx_models.PlayList;
import main.java.uk.ac.imperial.lsds.dx_models.Recommendation;
import main.java.uk.ac.imperial.lsds.dx_models.StatsTimeseries;
import main.java.uk.ac.imperial.lsds.dx_models.Track;
import main.java.uk.ac.imperial.lsds.dx_models.User;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;


/**
 * Generic Object - Mapping Controller - similar to Facade Pattern
 * All Accessor interface methods should be called here
 *  
 * @author pgaref
 *
 */
public class CassandraDxQueryController {
	
	static Logger  logger = Logger.getLogger(CassandraDxQueryController.class);
	static Counter trackCounter = new Counter("tracks");
	static Counter userCounter = new Counter("users");
	
	private Session clusterSession;
	private MappingManager manager;
	
	public CassandraDxQueryController(Session session){
		this.clusterSession = session;
		manager = new MappingManager (clusterSession);
	}
	
	/**
	 * User - Cassandra Object Mapping 
	 * @param user
	 */
	
	public void persist(User user) {
		Mapper<User> userMapper = manager.mapper(User.class);
		userMapper.save(user);
		//Counter ++
	}

	public User find(String email) {
		Mapper<User> userMapper = manager.mapper(User.class);
		return userMapper.get(email);
	}
	
	public List<User> getAllUsers() {
		UserAccessorI userAccessor = manager.createAccessor(UserAccessorI.class);
		Result<User> users = userAccessor.getAll();
		return users.all();
	}
	
	public List<User> getAllUsersAsync() throws InterruptedException, ExecutionException {
		UserAccessorI userAccessor = manager.createAccessor(UserAccessorI.class);
		ListenableFuture<Result<User>> users = userAccessor.getAllAsync();
		return users.get().all();
	}

	
	/**
	 * Track - Cassandra JPA
	 * @param Track
	 */
	
	public void persist(Track song) {
		Mapper<Track> trackMapper = manager.mapper(Track.class);
		trackMapper.save(song);
	}
	
	public void remove(Track song) {
		Mapper<Track> trackMapper = manager.mapper(Track.class);
		trackMapper.delete(song);
		//counter--
	}
	
	public Track findByTrackID(String id){
		Mapper<Track> trackMapper = manager.mapper(Track.class);
		return trackMapper.get(id);
	}
	
	public List<Track> findTrackbyTitle(String title) {
//		TrackAccessorI taccessor = manager.createAccessor(TrackAccessorI.class);
//		List<Track> t = taccessor.getbyTitle(title).all();
		Statement s = QueryBuilder.select()
				.all()
				.from("play_cassandra", "tracks")
				.where(eq("title",title));
		ResultSet re  = this.manager.getSession().execute(s);
		for(Row r : re.all()){
			System.out.println("row: "+ r);
		}
		//System.out.println("Got: "+ t);
		return null;
	}
	
	public List<Track> getAllTracks() {
		TrackAccessorI taccessor = manager.createAccessor(TrackAccessorI.class);
		return taccessor.getAll().all();
	}
	
	public List<Track> getAllTracksAsync() {
		TrackAccessorI taccessor = manager.createAccessor(TrackAccessorI.class);
		return taccessor.getAll().all();
	}
	
	public static List<Track> getAllTracksWithPagination() throws Exception {
		throw new Exception("Not implemented yet!!");
		//return null;
	}
	
	/**
	 * GENERIC counter - Cassandra JPA
	 * 
	 */
	
	public Counter getCounter(String key){
		Mapper<Counter> cmapper = manager.mapper(Counter.class);
		return cmapper.get(key);
	}
	
	public void increment(Counter counter) {
		CounterAccessorI caccessor = manager.createAccessor(CounterAccessorI.class);
		caccessor.incrementCounter(counter.getId());
	}
	
	public void decrement(Counter counter) {
		CounterAccessorI caccessor = manager.createAccessor(CounterAccessorI.class);
		caccessor.decrementCounter(counter.getId());
	}
	
	public long getCounterValue(String key){
		Mapper<Counter> cmapper = manager.mapper(Counter.class);
		return cmapper.get(key).getCounter();
	}
	
	/**
	 * Recommendation - Cassandra JPA
	 * @param Recommendation
	 * 
	 */
	public void persist(Recommendation r) {
		Mapper<Recommendation> userMapper = manager.mapper(Recommendation.class);
		userMapper.save(r);

	}
	
	public static List<Recommendation> getAllRecommendations(){
//		EntityManager em = getEntintyManagerCustom();
//		Query findQuery = em.createQuery("Select r from Recommendation r", Recommendation.class);
//		findQuery.setMaxResults(Integer.MAX_VALUE);
//		List<Recommendation> allRec = findQuery.getResultList();
//		em.close();
//		
//		logger.debug("\n ##############  Listing All Recommendations, Total Size:" + allRec.size() +" ############## \n ");
		return null;
	}
	
	public static Recommendation getUserRecc(String usermail){
//		EntityManager em = getEntintyManagerCustom();

		
//		Recommendation found = em.find(Recommendation.class, usermail);
//
//		if(found == null){
//			logger.debug("\n Recommendations for user : "+ usermail +" NOT FOUND!!!");
//			return null;
//		}
//		else{
//			return found;
//		}
		return null;

	}
	/**
	 * Playlist - Cassandra JPA
	 * @param Playlist
	 * 
	 */

	public void persist(PlayList p) {
		Mapper<PlayList> plMapper = manager.mapper(PlayList.class);
		plMapper.save(p);
	}
	
	public List<PlayList> getAllPlaylists(){
		PlaylistAccessorI plAccessor = manager.createAccessor(PlaylistAccessorI.class);
		Result<PlayList> playlists = plAccessor.getAll();
		return playlists.all();
	}
	public List<PlayList> getAllPlaylistsAsync() throws InterruptedException, ExecutionException{
		PlaylistAccessorI plAccessor = manager.createAccessor(PlaylistAccessorI.class);
		ListenableFuture<Result<PlayList>> playlists = plAccessor.getAllAsync();
		return playlists.get().all();
	}
	
	public List<PlayList> getUserPlayLists(String usermail){
		PlaylistAccessorI plAccessor = manager.createAccessor(PlaylistAccessorI.class);
		Result<PlayList> playlists = plAccessor.getUserPlaylists(usermail);
		return playlists.all();
	}
	
	public boolean deleteUserPlayListSong(UUID playlistid, String song){		
		Mapper<PlayList> plMapper = manager.mapper(PlayList.class);
		PlayList existing = plMapper.get(playlistid);
		if(existing == null)
			return false;
		List<String> tracksToUpdate = existing.getTracks();
		tracksToUpdate.remove(song);
		PlaylistAccessorI plAccessor = manager.createAccessor(PlaylistAccessorI.class);
		plAccessor.deleteUserPlayListSong(playlistid, tracksToUpdate);
		
		return (existing.getTracks().size() != tracksToUpdate.size());
	}
	
	/**
	 * New TimeSeries Stats - Cassandra JPA
	 * @param Stats
	 * 
	 */
	public void persist(StatsTimeseries s) {
		Mapper<StatsTimeseries> userMapper = manager.mapper(StatsTimeseries.class);
		userMapper.save(s);
	}
	
	
	public static List<StatsTimeseries> getAllStatsTimeseries(String statsID){
//		
//		EntityManager em = getEntintyManagerCustom();
//		Query findQuery = em.createNativeQuery("SELECT * FROM \"statseries\" WHERE \"id\"='"+statsID+"';", StatsTimeseries.class);
//		//Query findQuery = em.createQuery("Select s from StatsTimeseries s", StatsTimeseries.class);
//		findQuery.setMaxResults(Integer.MAX_VALUE);
//		List<StatsTimeseries> allStats = findQuery.getResultList();
//		em.close();
//		
//		logger.debug("\n ##############  Listing All StatsTimeseries, Total Size:" + allStats.size() +" ############## \n ");
		return null;
	}
	
	
	
	public static void main(String[] args) {
		ClusterManager cm = new ClusterManager("play_cassandra", 1, "146.179.131.141");
		CassandraDxQueryController dx = new CassandraDxQueryController(cm.getSession());
		
		/*
		List<User> allusers = dx.getAllUsers();
		System.out.println("Size:"+ allusers.size());
		
		List<PlayList> allLists = dx.getAllPlaylists();
		System.out.println("All list "+ allLists.size());
		*/
		List<PlayList> pgLists = dx.getUserPlayLists("pangaref@example.com");
		for(PlayList list : pgLists)
			System.out.println("pgList "+ list.toString());
		System.out.println("Deleted: "+ dx.deleteUserPlayListSong(pgLists.get(0).getId(), "Goodbye"));
		
		
		List<Track> byTitle = dx.findTrackbyTitle("Goodbye");
		System.out.println("Got By Title: "+ byTitle);
		
		
//		Counter userCount = dx.getCounter("user");
//		System.out.println("Counter "+ userCount );
//		long cval = dx.getCounterValue("user");
//		System.out.println("Cval"+cval);
		Counter userCount = new Counter("user");
		long cval;
		dx.increment(userCount);
		cval = dx.getCounterValue("user");
		System.out.println("After inc Cval = "+cval);
		dx.decrement(userCount);
		cval = dx.getCounterValue("user");
		System.out.println("After decr Cval = "+cval);
				
		cm.disconnect();
		System.out.println("Cluster Manager disconnected! ");
		
	}
}