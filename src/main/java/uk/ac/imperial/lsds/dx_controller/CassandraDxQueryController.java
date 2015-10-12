package main.java.uk.ac.imperial.lsds.dx_controller;

import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;

import main.java.uk.ac.imperial.lsds.dx_accessors.CounterAccessorI;
import main.java.uk.ac.imperial.lsds.dx_accessors.PlaylistAccessorI;
import main.java.uk.ac.imperial.lsds.dx_accessors.RecommendationAccessorI;
import main.java.uk.ac.imperial.lsds.dx_accessors.StatsTimeseriesAccessorI;
import main.java.uk.ac.imperial.lsds.dx_accessors.TrackAccessorI;
import main.java.uk.ac.imperial.lsds.dx_accessors.UserAccessorI;
import main.java.uk.ac.imperial.lsds.dx_models.Counter;
import main.java.uk.ac.imperial.lsds.dx_models.PlayList;
import main.java.uk.ac.imperial.lsds.dx_models.Recommendation;
import main.java.uk.ac.imperial.lsds.dx_models.StatsTimeseries;
import main.java.uk.ac.imperial.lsds.dx_models.Track;
import main.java.uk.ac.imperial.lsds.dx_models.User;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.common.util.concurrent.ListenableFuture;

//import static com.datastax.driver.core.querybuilder.QueryBuilder.*;


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
	static Counter playlistCounter = new Counter("playlists");
	
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
		this.increment(userCounter);
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
		this.increment(trackCounter);
	}
	
	public void remove(Track song) {
		Mapper<Track> trackMapper = manager.mapper(Track.class);
		trackMapper.delete(song);
		this.decrement(trackCounter);
	}
	
	public Track findByTrackID(String id){
		Mapper<Track> trackMapper = manager.mapper(Track.class);
		return trackMapper.get(id);
	}
	
	public List<Track> findTrackbyTitle(String title) {
		TrackAccessorI taccessor = manager.createAccessor(TrackAccessorI.class);
		return taccessor.getbyTitle(title).all();
//		Statement s = QueryBuilder.select()
//				.all()
//				.from("play_cassandra", "tracks")
//				.where(eq("title",title));
//		ResultSet re  = this.manager.getSession().execute(s);
//		for(Row r : re.all()){
//			System.out.println("row: "+ r);
//		}
	}
	
	public List<Track> getAllTracks() {
		TrackAccessorI taccessor = manager.createAccessor(TrackAccessorI.class);
		return taccessor.getAll().all();
	}
	
	public List<Track> getAllTracksAsync() {
		TrackAccessorI taccessor = manager.createAccessor(TrackAccessorI.class);
		return taccessor.getAll().all();
	}
	
	public List<Track> getTracksPage(int tracksNum) {
		TrackAccessorI taccessor = manager.createAccessor(TrackAccessorI.class);
		return taccessor.getTacksPage(tracksNum).all();
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
	
	public void incrementByValue(Counter counter, long val){
		CounterAccessorI caccessor = manager.createAccessor(CounterAccessorI.class);
		caccessor.increaseCounterByValue(counter.getId(), val);
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
		Mapper<Recommendation> recMapper = manager.mapper(Recommendation.class);
		recMapper.save(r);
	}
	
	public List<Recommendation> getAllRecommendations(){
		RecommendationAccessorI raccessor = manager.createAccessor(RecommendationAccessorI.class);
		return raccessor.getAll().all();
	}
	
	public List<Recommendation> getAllRecommendationsAsync(){
		RecommendationAccessorI raccessor = manager.createAccessor(RecommendationAccessorI.class);
		return raccessor.getAll().all();
	}
	
	public Recommendation getUserRecommendations(String usermail){
		Mapper<Recommendation> recMapper = manager.mapper(Recommendation.class);
		return recMapper.get(usermail);
	}
	/**
	 * Playlist - Cassandra JPA
	 * @param Playlist
	 * 
	 */

	public void persist(PlayList p) {
		Mapper<PlayList> plMapper = manager.mapper(PlayList.class);
		plMapper.save(p);
		this.increment(playlistCounter);
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
		return plAccessor.getUserPlaylists(usermail).all();
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
	 * New TimeSeries Stats - Cassandra Object Mapper
	 * @param Stats
	 * 
	 */
	public void persist(StatsTimeseries s) {
		Mapper<StatsTimeseries> userMapper = manager.mapper(StatsTimeseries.class);
		userMapper.save(s);
	}
	
	
	public List<StatsTimeseries> getAllStatsTimeseries(String statsID){
		StatsTimeseriesAccessorI saccessor = manager.createAccessor(StatsTimeseriesAccessorI.class);
		return saccessor.getAll().all();
	}
	public List<StatsTimeseries> getAllStatsTimeseriesAsync(String statsID) throws InterruptedException, ExecutionException{
		StatsTimeseriesAccessorI saccessor = manager.createAccessor(StatsTimeseriesAccessorI.class);
		return saccessor.getAllAsync().get().all();
	}
	
	
	public static void main(String[] args) {
		//ClusterManager cm = new ClusterManager("play_cassandra", 1, "146.179.131.141");
		ClusterManager cm = new ClusterManager("play_cassandra", 1, "155.198.198.12");
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
		System.out.println("Got By Title: Goodbye , TOTAL="+ byTitle.size());
		
		
//		Counter userCount = dx.getCounter("user");
//		System.out.println("Counter "+ userCount );
//		long cval = dx.getCounterValue("user");
//		System.out.println("Cval"+cval);

/*		Counter userCount = new Counter("user");
		long cval;
		dx.increment(userCount);
		cval = dx.getCounterValue("user");
		System.out.println("After inc Cval = "+cval);
		dx.decrement(userCount);
		cval = dx.getCounterValue("user");
		System.out.println("After decr Cval = "+cval);

		System.out.println("Rec List Size: "+ dx.getAllRecommendations().size());
		
		System.out.println("pangaref Recs: "+ dx.getUserRecommendations("pangaref@example.com").email);
		System.out.println("pangaref Recs: "+ dx.getUserRecommendations("pangaref@example.com").toString());
*/
		StatsTimeseries ts = new StatsTimeseries("pgSeriesTest");
		ts.getMetricsMap().put("cpu-util", "90%");
		ts.getMetricsMap().put("ram", "50%");
		dx.persist(ts);
		
		ts = new StatsTimeseries("pgSeriesTest");
		ts.getMetricsMap().put("cpu-util", "90%");
		ts.getMetricsMap().put("ram", "50%");
		dx.persist(ts);
				
		cm.disconnect();
		System.out.println("Cluster Manager disconnected! ");
		
	}
}