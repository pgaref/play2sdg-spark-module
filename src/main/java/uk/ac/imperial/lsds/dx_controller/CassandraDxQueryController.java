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
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;



/**
 * User Controller for Apache Cassandra back-end
 * @author pg1712
 *
 */

public class CassandraDxQueryController {
	
	static Logger  logger = Logger.getLogger(CassandraDxQueryController.class);
	static Counter songCounter = new Counter("tracks");
	static Counter userCounter = new Counter("user");
	
	private Session clusterSession;
	private MappingManager manager;
	
	public CassandraDxQueryController(Session session){
		this.clusterSession = session;
		manager = new MappingManager (clusterSession);
	}
	
	/**
	 * User - Cassandra JPA 
	 * @param user
	 */
	
	public void persist(User user) {
		Mapper<User> userMapper = manager.mapper(User.class);
		userMapper.save(user);
		
	}

	public User find(String email) {
		Mapper<User> userMapper = manager.mapper(User.class);
		return userMapper.get(email);
	}
	
	public List<User> listAllUsers() {
		UserAccessorI userAccessor = manager.createAccessor(UserAccessorI.class);
		Result<User> users = userAccessor.getAll();
		return users.all();
	}
	
	public List<User> listAllUsersAsync() throws InterruptedException, ExecutionException {
		UserAccessorI userAccessor = manager.createAccessor(UserAccessorI.class);
		ListenableFuture<Result<User>> users = userAccessor.getAllAsync();
		return users.get().all();
	}

	
	/**
	 * Track - Cassandra JPA
	 * @param Track
	 */
	
	public void persist(Track song) {
		Mapper<Track> userMapper = manager.mapper(Track.class);
		userMapper.save(song);
	}
	
	public static void remove(Track song) {
//		EntityManager em = getEntintyManagerCustom();
//		em.remove(song);
//		decrement(songCounter);
//		em.close();
//		logger.debug("\n Track: " + song.getTitle() + " record REMOVED using persistence unit ->" +getEntintyManagerCustom());
	}
	
	public static Track findByTrackID(String id){
//		EntityManager em = getEntintyManagerCustom();
//		//Track t = em.find(Track.class, id);
//		Query q = em.createNativeQuery("SELECT * FROM \"tracks\" WHERE \"key\"='"+id+"';", Track.class);
//		q.setMaxResults(1);
//		List<Track> t = q.getResultList();
//		if(t == null)
//			logger.debug("Tack "+ id +" not found in the database!");
//		return t.get(0);
		return null;
		
	}
	
	public static Track findTrackbyTitle(String title) {
//		/*
//		 * Use NativeQuery to avoid Strings being used as cassandra keywords!
//		 * https://github.com/impetus-opensource/Kundera/issues/151
//		 * Incompatible with CQL2 !!
//		 */
//		
//		EntityManager em = getEntintyManagerCustom();
//        
//		Query findQuery = em
//				.createNativeQuery("SELECT * FROM tracks WHERE title = '"+ title+"';", Track.class);
//		if(findQuery.getResultList().size() == 0){
//			logger.debug("Could not find any Tracks with title: "+ title);
//			return null;	
//		}
//		if(findQuery.getResultList().size() >1 )
//			logger.warn("Query Returned more than one Tracks with title: "+ title);
//		return (Track) findQuery.getResultList().get(0);
		return null;
	}
	
	public static List<Track> listAllTracks() {
//		EntityManager em = getEntintyManagerCustom();
//		Query findQuery = em.createQuery("Select s from Track s", Track.class);
//		findQuery.setMaxResults(Integer.MAX_VALUE);
//		List<Track> allSongs = findQuery.getResultList();
//		em.close();
//		
//		logger.debug("\n ##############  Listing All Track, Total Size:" + allSongs.size() +" ############## \n ");
////		for (Song s : allSongs) {
////			logger.debug("\n Got Song: \n" + s);
////		}
		return null;
	}
	
	public static List<Track> listAllTracksWithPagination() {
		
//		EntityManager em = getEntintyManagerCustom();
//		List<Track> allTracks = new ArrayList<Track>();
//		
//		Query firstPage =em.createQuery("Select s from Track s", Track.class);
//		firstPage.setMaxResults(5000);
//		
//		@SuppressWarnings("unchecked")
//		List<Track> result = firstPage.getResultList();
//		logger.debug("\n ##############  Listing First Track page ############## \n ");
//		allTracks.addAll(result);
//		
//		while(allTracks.size() < DatastaxCassandraQueryController.getCounterValue("tracks") ){
//			em = getEntintyManagerCustom();
//			Query findQuery = em
//				.createNativeQuery("SELECT * FROM tracks WHERE token(key) > token('"+ allTracks.get(allTracks.size()-1).getTrack_id() +"') LIMIT 5000;", Track.class);
//			findQuery.setMaxResults(5000);
//			@SuppressWarnings("unchecked")
//			List<Track> nextPageTracks = findQuery. getResultList();
//			allTracks.addAll(nextPageTracks);
//			logger.debug("\n ##############  Listing Next Track page, Current Size: "+ allTracks.size() +"  After TrackID: "+ allTracks.get(allTracks.size()-1).getTrack_id() +" ############## \n ");
//
//		} 
//		
		return null;
	}
	
	/**
	 * GENERIC counter - Cassandra JPA
	 * @param Generic counter
	 * 
	 */
	
	public static void increment(Counter counter) {
//		EntityManager em = getEntintyManagerCustom();
//		Counter tmp = em.find(Counter.class, counter.getId());
//		if(tmp == null){
//			if(counter.getId() == "tracks"){
//				songCounter.incrementCounter();
//				em.persist(songCounter);
//				logger.debug("Generic: " + songCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
//			}
//			else if(counter.getId() == "user"){
//				userCounter.incrementCounter();
//				em.persist(userCounter);
//				logger.debug("Generic: " + userCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
//			}
//		}
//		else{
//			if(counter.getId() == "tracks"){
//				songCounter.incrementCounter();
//				em.merge(songCounter);
//				logger.debug("Generic: " + songCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
//			}
//			else if(counter.getId() == "user"){
//				userCounter.incrementCounter();
//				em.merge(userCounter);
//				logger.debug("Generic: " + userCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
//			}
//		}

	}
	
	public static void decrement(Counter counter) {
//		EntityManager em = getEntintyManagerCustom();
//		Counter tmp = em.find(Counter.class, counter.getId());
//		if(tmp == null){
//			if(counter.getId() == "tracks"){
//				songCounter.decrementCounter();
//				em.persist(songCounter);
//				logger.debug("Generic: " + songCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
//			}
//			else if(counter.getId() == "user"){
//				userCounter.decrementCounter();
//				em.persist(userCounter);
//				logger.debug("Generic: " + userCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
//			}
//		}
//		else{
//			if(counter.getId() == "tracks"){
//				songCounter.decrementCounter();
//				em.merge(songCounter);
//				logger.debug("Generic: " + songCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
//			}
//			else if(counter.getId() == "user"){
//				userCounter.decrementCounter();
//				em.merge(userCounter);
//				logger.debug("Generic: " + userCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
//			}
//			
//		}
//		em.close();

	}
	
	public static int getCounterValue(String id){

		return 0;
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
	
	public static List<Recommendation> listAllRecommendations(){
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
		Mapper<PlayList> userMapper = manager.mapper(PlayList.class);
		userMapper.save(p);
	}
	
	public static List<PlayList> listAllPlaylists(){

		return null;
	}
	
	public static List<PlayList> getUserPlayLists(String usermail){
//		EntityManager em = getEntintyManagerCustom();
//		Query findQuery = em
//				.createQuery("Select p from PlayList p where p.usermail = " +usermail );
//		@SuppressWarnings("unchecked")
//		List<PlayList> tmp =  (List<PlayList>) findQuery.getResultList();
//		
//		//Avoid null saved playlists - scala Option is another alternative to catch null pointers
//		if(tmp == null){
//			logger.debug("User: "+ usermail + " has NO playlists!");
//			return null;
//		}	
//		//Avoid null pointers in SCALA viewsongs!
//		for(PlayList p : tmp ){
//			if(p.getTracks() == null)
//				p.setTracks(new ArrayList<String>());
//		}
//		logger.debug("\n\n---->>> getUserPlayLists QUery returned: "+ tmp) ;
		return null;
	}
	
	public static boolean deleteUserPlayListSong(UUID playlistid, String song){
//		EntityManager em = getEntintyManagerCustom();
//		Query findQuery = em.createNativeQuery("Select * from playlists  where token(id) = token("+ playlistid + ");", PlayList.class );
//		@SuppressWarnings("unchecked")
//		List<PlayList> p = findQuery.getResultList();
//		assert(p.size() != 1);
//		for(int i =0 ; i < p.get(0).getTracks().size(); i++){
//			if(p.get(0).getTracks().get(i).compareTo(song) == 0){
//				p.get(0).getTracks().remove(i);
//				em.merge(p.get(0));
//				return true;
//			}
//		}			
		return false;
	}


//	public static void persist(Stats s) {	
//	}
//	
//	public static List<Stats> getAllStats(){
////		EntityManager em = getEntintyManagerCustom();
////		Query findQuery = em.createQuery("Select s from Stats s", Stats.class);
////		findQuery.setMaxResults(Integer.MAX_VALUE);
////		List<Stats> allStats = findQuery.getResultList();
////		em.close();
////		
////		logger.debug("\n ##############  Listing All Stats, Total Size:" + allStats.size() +" ############## \n ");
//		return null;
//	}
	
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
	
	//ALTER KEYSPACE play_cassandra WITH REPLICATION =   { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
	public static int UpdateKeyspaceRF(String ks,int rf){
//		EntityManager em = getEntintyManagerCustom();
//		Query alterQ = em.createQuery("ALTER KEYSPACE "+ks +" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : " + rf +"};");
//		int result = alterQ.executeUpdate();
//		em.close();
//		
//		logger.debug("\n ##############  Altering keyspace :"+ ks  + " with new Replication factor:" + rf +" ############## \n ");
		return 0;
	}
	
	
	
	public static void main(String[] args) {
		ClusterManager cm = new ClusterManager("play_cassandra", 1, "146.179.131.141");
		CassandraDxQueryController dx = new CassandraDxQueryController(cm.getSession());
		
		List<User> got = dx.listAllUsers();
		System.out.println("Size:"+ got.size());
		//System.out.println(got);
		
	}
}