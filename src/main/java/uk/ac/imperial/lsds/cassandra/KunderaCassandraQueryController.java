package main.java.uk.ac.imperial.lsds.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import main.java.uk.ac.imperial.lsds.models.Counter;
import main.java.uk.ac.imperial.lsds.models.PlayList;
import main.java.uk.ac.imperial.lsds.models.Recommendation;
import main.java.uk.ac.imperial.lsds.models.Stats;
import main.java.uk.ac.imperial.lsds.models.StatsTimeseries;
import main.java.uk.ac.imperial.lsds.models.Track;
import main.java.uk.ac.imperial.lsds.models.User;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.thrift.ThriftClient;
import com.impetus.kundera.client.Client;


/**
 * User Controller for Apache Cassandra back-end
 * @author pg1712
 *
 */

public class KunderaCassandraQueryController {
	
	static Logger  logger = Logger.getLogger(KunderaCassandraQueryController.class);
	static EntityManagerFactory emf;
	
	static Counter songCounter = new Counter("tracks");
	static Counter userCounter = new Counter("user");
	
	/**
	 * User - Cassandra JPA
	 * @param user
	 */
	
//	
	public static void persist(User user) {
		EntityManager em = getEntintyManagerCustom();
		User tmp = em.find(User.class, user.getEmail());
		if(tmp == null){
			em.persist(user);
			logger.debug("\n User: " + user.getEmail() + " record persisted using persistence unit -> cassandra_pu");
			increment(userCounter);
		}
		else{
			em.merge(user);
			logger.debug("\n User: " + user.getEmail() + " record merged using persistence unit -> cassandra_pu");
		}
		em.close();
	}

	public static User findbyEmail(String email) {
		EntityManager em = getEntintyManagerCustom();
		User user = em.find(User.class, email);
		em.close();
		
		boolean userStr =  ( user == null ? false : true );
		logger.debug("\n Looking for User: " +email + " in cassandra database... found: "
				+ userStr);
		return user;
	}
	
	public static List<User> listAllUsers() {
		EntityManager em = getEntintyManagerCustom();;
		Query findQuery = em.createQuery("Select p from User p", User.class);
		findQuery.setMaxResults(Integer.MAX_VALUE);
		@SuppressWarnings("unchecked")
		List<User> allUsers = findQuery.getResultList();
		em.close();
		
		logger.debug("\n-------- Listing All Users -------- ");
		for (User u : allUsers) {
			logger.debug(" Got User: " + u.username);
		}
		logger.debug("\n ---------------- ");
		return allUsers;
	}

	
	/**
	 * Track - Cassandra JPA
	 * @param Track
	 */
	
	public static void persist(Track song) {
		EntityManager em = getEntintyManagerCustom();
		Track tmp = em.find(Track.class, song.getTrack_id());
		if(tmp == null){
			em.persist(song);
			increment(songCounter);
			logger.debug("\n Track: " + song.getTitle() + " record persisted using persistence unit -> " + getEntintyManagerCustom());
		}
		else{
			em.merge(song);
			logger.debug("\n Track: " + song.getTitle() + " record merged using persistence unit ->" +getEntintyManagerCustom());
		}
		em.close();
		
	}
	
	public static void remove(Track song) {
		EntityManager em = getEntintyManagerCustom();
		em.remove(song);
		decrement(songCounter);
		em.close();
		logger.debug("\n Track: " + song.getTitle() + " record REMOVED using persistence unit ->" +getEntintyManagerCustom());
	}
	
	public static Track findByTrackID(String id){
		EntityManager em = getEntintyManagerCustom();
		//Track t = em.find(Track.class, id);
		Query q = em.createNativeQuery("SELECT * FROM \"tracks\" WHERE \"key\"='"+id+"';", Track.class);
		q.setMaxResults(1);
		List<Track> t = q.getResultList();
		if(t == null)
			logger.debug("Tack "+ id +" not found in the database!");
		return t.get(0);
		
	}
	
	public static Track findTrackbyTitle(String title) {
		/*
		 * Use NativeQuery to avoid Strings being used as cassandra keywords!
		 * https://github.com/impetus-opensource/Kundera/issues/151
		 * Incompatible with CQL2 !!
		 */
		
		EntityManager em = getEntintyManagerCustom();
        
		Query findQuery = em
				.createNativeQuery("SELECT * FROM tracks WHERE title = '"+ title+"';", Track.class);
		if(findQuery.getResultList().size() == 0){
			logger.debug("Could not find any Tracks with title: "+ title);
			return null;	
		}
		if(findQuery.getResultList().size() >1 )
			logger.warn("Query Returned more than one Tracks with title: "+ title);
		return (Track) findQuery.getResultList().get(0);
	}
	
	public static List<Track> listAllTracks() {
		EntityManager em = getEntintyManagerCustom();
		Query findQuery = em.createQuery("Select s from Track s", Track.class);
		findQuery.setMaxResults(Integer.MAX_VALUE);
		List<Track> allSongs = findQuery.getResultList();
		em.close();
		
		logger.debug("\n ##############  Listing All Track, Total Size:" + allSongs.size() +" ############## \n ");
//		for (Song s : allSongs) {
//			logger.debug("\n Got Song: \n" + s);
//		}
		return allSongs;
	}
	
	public static List<Track> listAllTracksWithPagination() {
		
		EntityManager em = getEntintyManagerCustom();
		List<Track> allTracks = new ArrayList<Track>();
		
		Query firstPage =em.createQuery("Select s from Track s", Track.class);
		firstPage.setMaxResults(5000);
		
		@SuppressWarnings("unchecked")
		List<Track> result = firstPage.getResultList();
		logger.debug("\n ##############  Listing First Track page ############## \n ");
		allTracks.addAll(result);
		
		while(allTracks.size() < KunderaCassandraQueryController.getCounterValue("tracks") ){
			em = getEntintyManagerCustom();
			Query findQuery = em
				.createNativeQuery("SELECT * FROM tracks WHERE token(key) > token('"+ allTracks.get(allTracks.size()-1).getTrack_id() +"') LIMIT 5000;", Track.class);
			findQuery.setMaxResults(5000);
			@SuppressWarnings("unchecked")
			List<Track> nextPageTracks = findQuery. getResultList();
			allTracks.addAll(nextPageTracks);
			logger.debug("\n ##############  Listing Next Track page, Current Size: "+ allTracks.size() +"  After TrackID: "+ allTracks.get(allTracks.size()-1).getTrack_id() +" ############## \n ");

		} 
		
		return allTracks;
	}
	
	/**
	 * GENERIC counter - Cassandra JPA
	 * @param Generic counter
	 * 
	 */
	
	public static void increment(Counter counter) {
		EntityManager em = getEntintyManagerCustom();
		Counter tmp = em.find(Counter.class, counter.getId());
		if(tmp == null){
			if(counter.getId() == "tracks"){
				songCounter.incrementCounter();
				em.persist(songCounter);
				logger.debug("Generic: " + songCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
			}
			else if(counter.getId() == "user"){
				userCounter.incrementCounter();
				em.persist(userCounter);
				logger.debug("Generic: " + userCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
			}
		}
		else{
			if(counter.getId() == "tracks"){
				songCounter.incrementCounter();
				em.merge(songCounter);
				logger.debug("Generic: " + songCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
			}
			else if(counter.getId() == "user"){
				userCounter.incrementCounter();
				em.merge(userCounter);
				logger.debug("Generic: " + userCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
			}
		}

	}
	
	public static void decrement(Counter counter) {
		EntityManager em = getEntintyManagerCustom();
		Counter tmp = em.find(Counter.class, counter.getId());
		if(tmp == null){
			if(counter.getId() == "tracks"){
				songCounter.decrementCounter();
				em.persist(songCounter);
				logger.debug("Generic: " + songCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
			}
			else if(counter.getId() == "user"){
				userCounter.decrementCounter();
				em.persist(userCounter);
				logger.debug("Generic: " + userCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
			}
		}
		else{
			if(counter.getId() == "tracks"){
				songCounter.decrementCounter();
				em.merge(songCounter);
				logger.debug("Generic: " + songCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
			}
			else if(counter.getId() == "user"){
				userCounter.decrementCounter();
				em.merge(userCounter);
				logger.debug("Generic: " + userCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
			}
			
		}
		em.close();

	}
	
	public static int getCounterValue(String id){
		EntityManager em = getEntintyManagerCustom();
		Counter tmp = em.find(Counter.class, id);
		if(tmp == null){
			logger.debug("\n Counter: "+ id +" NOT FOUND!!!");
			return 0;
		}
		else{
			return tmp.getCounter();
		}

	}
	
	/**
	 * Recommendation - Cassandra JPA
	 * @param Recommendation
	 * 
	 */
	public static void persist(Recommendation r) {
		EntityManager em = getEntintyManagerCustom();
		//Query findQuery = em.createNativeQuery("Select * from recommendations where email = '" +  r.getEmail() +"';", Recommendation.class);
		//findQuery.setMaxResults(1);	
		try{
//			@SuppressWarnings("unchecked")
//			List<Recommendation> results = findQuery.getResultList();
//			Recommendation tmp =  ( ((results == null) || (results.size() ==0)) ? null :  results.get(0) );
//			if(tmp == null){
				em.persist(r);
				logger.debug("\n Recommendation for : " + r.getEmail() + " record persisted using persistence unit -> cassandra_pu");
//			}
//			else{
//				if (tmp.getRecList() != null)
//					r.getRecList().putAll(tmp.getRecList());
//				em.merge(r);
//				logger.debug("\n Recommendation for : " + r.getEmail() + " record merged using persistence unit -> cassandra_pu");
//			}
		}catch(javax.persistence.PersistenceException ex){
			logger.error("-> cassandra - PersistenceException");
		}catch(com.impetus.kundera.KunderaException ex){
			logger.error("-> cassandra - WriteTimeout");
		}catch(Exception ex){
			logger.error("-> cassandra - Exception: "+ ex.getClass());
		}
	}
	
	public static List<Recommendation> listAllRecommendations(){
		EntityManager em = getEntintyManagerCustom();
		Query findQuery = em.createQuery("Select r from Recommendation r", Recommendation.class);
		findQuery.setMaxResults(Integer.MAX_VALUE);
		List<Recommendation> allRec = findQuery.getResultList();
		em.close();
		
		logger.debug("\n ##############  Listing All Recommendations, Total Size:" + allRec.size() +" ############## \n ");
		return allRec;
	}
	
	public static Recommendation getUserRecc(String usermail){
		EntityManager em = getEntintyManagerCustom();
//		Query q = em.createNativeQuery("SELECT * FROM \"recommendations\" WHERE \"email\"='"+usermail+"';", Recommendation.class);
//		q.setMaxResults(1);
//		@SuppressWarnings("unchecked")
//		List<Recommendation> tmp = q.getResultList();
		
		Recommendation found = em.find(Recommendation.class, usermail);

		if(found == null){
			logger.debug("\n Recommendations for user : "+ usermail +" NOT FOUND!!!");
			return null;
		}
		else{
			return found;
		}

	}
	/**
	 * Playlist - Cassandra JPA
	 * @param Playlist
	 * 
	 */

	public static void persist(PlayList p) {
		EntityManager em = getEntintyManagerCustom();
		PlayList tmp = em.find(PlayList.class, p.getId());
		if(tmp == null){
			em.persist(p);
			logger.debug("\n PlayList: " + p.getId() + " record persisted using persistence unit -> cassandra_pu");
		}
		else{
			em.merge(p);
			logger.debug("\n PlayList: " + p.getId() + " record merged using persistence unit -> cassandra_pu");
		}
		
	}
	
	public static List<PlayList> listAllPlaylists(){
		EntityManager em = getEntintyManagerCustom();
		Query findQuery = em.createQuery("Select p from PlayList p", PlayList.class);
		findQuery.setMaxResults(Integer.MAX_VALUE);
		List<PlayList> allPlaylists= findQuery.getResultList();
		em.close();
		return allPlaylists;
	}
	
	public static List<PlayList> getUserPlayLists(String usermail){
		EntityManager em = getEntintyManagerCustom();
		Query findQuery = em
				.createQuery("Select p from PlayList p where p.usermail = " +usermail );
		@SuppressWarnings("unchecked")
		List<PlayList> tmp =  (List<PlayList>) findQuery.getResultList();
		
		//Avoid null saved playlists - scala Option is another alternative to catch null pointers
		if(tmp == null){
			logger.debug("User: "+ usermail + " has NO playlists!");
			return null;
		}	
		//Avoid null pointers in SCALA viewsongs!
		for(PlayList p : tmp ){
			if(p.getTracks() == null)
				p.setTracks(new ArrayList<String>());
		}
		logger.debug("\n\n---->>> getUserPlayLists QUery returned: "+ tmp) ;
		return tmp;
	}
	
	public static boolean deleteUserPlayListSong(UUID playlistid, String song){
		EntityManager em = getEntintyManagerCustom();
		Query findQuery = em.createNativeQuery("Select * from playlists  where token(id) = token("+ playlistid + ");", PlayList.class );
		@SuppressWarnings("unchecked")
		List<PlayList> p = findQuery.getResultList();
		assert(p.size() != 1);
		for(int i =0 ; i < p.get(0).getTracks().size(); i++){
			if(p.get(0).getTracks().get(i).compareTo(song) == 0){
				p.get(0).getTracks().remove(i);
				em.merge(p.get(0));
				return true;
			}
		}			
		return false;
	}

	
	/**
	 * Stats - Cassandra JPA
	 * @param Stats
	 * 
	 */
	public static void persist(Stats s) {
		EntityManager em = getEntintyManagerCustom();
		Stats tmp = em.find(Stats.class, s.getId());
		if(tmp == null){
			em.persist(s);
			logger.debug("\n Stats for : " + s.getId() + " persisted using persistence unit -> cassandra_pu");
		}
		else{
			/*
			 * Update the existing Stat object with fresh values
			 * The other way round would cause old stats data to remain untouched!!
			 */
			tmp.setTimestamp(s.getTimestamp());
			tmp.getStatsMap().putAll(s.getStatsMap());
			em.merge(tmp);
			logger.debug("\n Stats for : " + tmp.getId() + " record merged using persistence unit -> cassandra_pu");
		}
		em.close();	
	}
	
	public static List<Stats> getAllStats(){
		EntityManager em = getEntintyManagerCustom();
		Query findQuery = em.createQuery("Select s from Stats s", Stats.class);
		findQuery.setMaxResults(Integer.MAX_VALUE);
		List<Stats> allStats = findQuery.getResultList();
		em.close();
		
		logger.debug("\n ##############  Listing All Stats, Total Size:" + allStats.size() +" ############## \n ");
		return allStats;
	}
	
	/**
	 * New TimeSeries Stats - Cassandra JPA
	 * @param Stats
	 * 
	 */
	public static void persist(StatsTimeseries s) {
		EntityManager em = getEntintyManagerCustom();

		em.merge(s);
		logger.debug("\n StatsTimeseries for : " + s.getId()
				+ " record persisted using persistence unit -> cassandra_pu");
		em.close();
	}
	
	
	public static List<StatsTimeseries> getAllStatsTimeseries(String statsID){
		
		EntityManager em = getEntintyManagerCustom();
		Query findQuery = em.createNativeQuery("SELECT * FROM \"statseries\" WHERE \"id\"='"+statsID+"';", StatsTimeseries.class);
		//Query findQuery = em.createQuery("Select s from StatsTimeseries s", StatsTimeseries.class);
		findQuery.setMaxResults(Integer.MAX_VALUE);
		List<StatsTimeseries> allStats = findQuery.getResultList();
		em.close();
		
		logger.debug("\n ##############  Listing All StatsTimeseries, Total Size:" + allStats.size() +" ############## \n ");
		return allStats;
	}
	
	//ALTER KEYSPACE play_cassandra WITH REPLICATION =   { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
	public static int UpdateKeyspaceRF(String ks,int rf){
		EntityManager em = getEntintyManagerCustom();
		Query alterQ = em.createQuery("ALTER KEYSPACE "+ks +" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : " + rf +"};");
		int result = alterQ.executeUpdate();
		em.close();
		
		logger.debug("\n ##############  Altering keyspace :"+ ks  + " with new Replication factor:" + rf +" ############## \n ");
		return result;
	}
	
	/**
	 * Entity Manager Factory and Wrapper
	 * @return
	 */
	
	private static EntityManager getEntintyManagerCustom() {	
		emf = KunderaCassandraQueryController.getEM();
		EntityManager em = emf.createEntityManager();
		
		return em;
	}
	
	
	private static EntityManagerFactory getEM() {
//		logger.setLevel(Level.WARN);
		if (emf == null) {
			Map<String, String> propertyMap = new HashMap<String, String>();
	        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
	        propertyMap.put("kundera.batch.size", "5");
			emf = Persistence.createEntityManagerFactory("cassandra_pu", propertyMap);
			logger.debug("\n emf"+ emf.toString());
		}
		return emf;
	}	
	
	public static void main(String[] args) {
		System.out.println("Tacks Counter: "+ KunderaCassandraQueryController.getCounterValue("tracks"));
		System.out.println("Looking for user pgaref : found =  "+ KunderaCassandraQueryController.findbyEmail("pgaref@example.com").toString());
		
	}
}