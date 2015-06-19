package main.java.uk.ac.imperial.lsds.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.impetus.client.cassandra.common.CassandraConstants;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import main.java.uk.ac.imperial.lsds.models.Counter;
import main.java.uk.ac.imperial.lsds.models.PlayList;
import main.java.uk.ac.imperial.lsds.models.Recommendation;
import main.java.uk.ac.imperial.lsds.models.Stats;
import main.java.uk.ac.imperial.lsds.models.Track;
import main.java.uk.ac.imperial.lsds.models.User;


/**
 * User Controller for Apache Cassandra back-end
 * @author pg1712
 *
 */

public class CassandraQueryController {
	
	static Logger  logger = Logger.getLogger("main.java.uk.ac.imperial.lsds.utils.CassandraController");
	static EntityManagerFactory emf;
	
	static Counter songCounter = new Counter("tracks");
	static Counter userCounter = new Counter("user");
	
	/**
	 * User - Cassandra JPA
	 * @param user
	 */
	
//	
	public static void persist(User user) {
		EntityManager em = getEm();
		User tmp = em.find(User.class, user.getEmail());
		if(tmp == null){
			em.persist(user);
			increment(userCounter);
		}
		else
			em.merge(user);
		em.close();
		logger.debug("\n User: " + user.getEmail() + " record persisted using persistence unit -> cassandra_pu");
	}

	public static User findbyEmail(String email) {
		EntityManager em = getEm();
		User user = em.find(User.class, email);
		em.close();
		
		boolean userStr =  ( user == null ? false : true );
		logger.debug("\n Looking for User: " +email + " in cassandra database... found: "
				+ userStr);
		return user;
	}
	
	public static List<User> listAllUsers() {
		EntityManager em = getEm();;
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
		EntityManager em = getEm();
		Track tmp = em.find(Track.class, song.getTrack_id());
		if(tmp == null){
			em.persist(song);
			increment(songCounter);
			logger.debug("\n Track: " + song.getTitle() + " record persisted using persistence unit -> " + getEmf().getProperties());
		}
		else{
			em.merge(song);
			logger.debug("\n Track: " + song.getTitle() + " record merged using persistence unit ->" +getEmf().getProperties());
		}
		em.close();
		
	}
	
	public static void remove(Track song) {
		EntityManager em = getEm();
		em.remove(song);
		decrement(songCounter);
		em.close();
		logger.debug("\n Track: " + song.getTitle() + " record REMOVED using persistence unit ->" +getEmf().getProperties());
	}
	
	
	public static Track findTrackbyTitle(String title) {
		/*
		 * Use NativeQuery to avoid Strings being used as cassandra keywords!
		 * https://github.com/impetus-opensource/Kundera/issues/151
		 * Incompatible with CQL2 !!
		 */
		
		EntityManager em = getEm();
        
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
		EntityManager em = getEm();
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
	
	/**
	 * GENERIC counter - Cassandra JPA
	 * @param Generic counter
	 * 
	 */
	
	public static void increment(Counter counter) {
		EntityManager em = getEm();
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
		em.close();
		
		
	}
	
	public static void decrement(Counter counter) {
		EntityManager em = getEm();
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
		EntityManager em = getEm();
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
		EntityManager em = getEm();
		Recommendation tmp = em.find(Recommendation.class, r.getEmail());
		if(tmp == null){
			em.persist(r);
			System.out.println("\n Recommendation for : " + r.getEmail() + " record persisted using persistence unit -> cassandra_pu");
		}
		else{
			r.getRecList().putAll(tmp.getRecList());
			em.merge(r);
			System.out.println("\n Recommendation for : " + r.getEmail() + " record merged using persistence unit -> cassandra_pu");
		}
		em.close();	
	}
	
	public static List<Recommendation> listAllRecommendations(){
		EntityManager em = getEm();
		Query findQuery = em.createQuery("Select r from Recommendation r", Recommendation.class);
		findQuery.setMaxResults(Integer.MAX_VALUE);
		List<Recommendation> allRec = findQuery.getResultList();
		em.close();
		
		logger.debug("\n ##############  Listing All Recommendations, Total Size:" + allRec.size() +" ############## \n ");
		return allRec;
	}
	
	public static Recommendation getUserRecc(String usermail){
		EntityManager em = getEmf().createEntityManager();
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
		EntityManager em = getEm();
		PlayList tmp = em.find(PlayList.class, p.getId());
		if(tmp == null){
			em.persist(p);
			System.out.println("\n PlayList: " + p.getId() + " record persisted using persistence unit -> cassandra_pu");
		}
		else{
			em.merge(p);
			System.out.println("\n PlayList: " + p.getId() + " record merged using persistence unit -> cassandra_pu");
		}
		em.close();
		
	}
	
	public static List<PlayList> listAllPlaylists(){
		EntityManager em = getEmf().createEntityManager();
		Query findQuery = em.createQuery("Select p from PlayList p", PlayList.class);
		findQuery.setMaxResults(Integer.MAX_VALUE);
		List<PlayList> allPlaylists= findQuery.getResultList();
		em.close();
		return allPlaylists;
	}


	
	/**
	 * Stats - Cassandra JPA
	 * @param Stats
	 * 
	 */
	public static void persist(Stats s) {
		EntityManager em = getEm();
		Stats tmp = em.find(Stats.class, s.getId());
		if(tmp == null){
			em.persist(s);
			System.out.println("\n Recommendation for : " + s.getId() + " record persisted using persistence unit -> cassandra_pu");
		}
		else{
			s.getStatsMap().putAll(tmp.getStatsMap());
			em.merge(s);
			System.out.println("\n Recommendation for : " + s.getId() + " record merged using persistence unit -> cassandra_pu");
		}
		em.close();	
	}
	
	public static List<Stats> getAllStats(){
		EntityManager em = getEm();
		Query findQuery = em.createQuery("Select s from Stats s", Stats.class);
		findQuery.setMaxResults(Integer.MAX_VALUE);
		List<Stats> allStats = findQuery.getResultList();
		em.close();
		
		logger.debug("\n ##############  Listing All Stats, Total Size:" + allStats.size() +" ############## \n ");
		return allStats;
	}
	
	//ALTER KEYSPACE play_cassandra WITH REPLICATION =   { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
	public static int UpdateKeyspaceRF(String ks,int rf){
		EntityManager em = getEm();
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
	
	private static EntityManager getEm() {
		logger.setLevel(Level.INFO);	
		if (emf == null) {
			EntityManager em = getEmf().createEntityManager();
			em.setProperty("cql.version", "3.0.0");
			logger.debug("\n emf"+ emf.toString());
		}
		emf =  getEmf();
		EntityManager em = emf.createEntityManager();
		em.setProperty("cql.version", "3.0.0");
		return em;
	}
	
	
	private static EntityManagerFactory getEmf() {
		logger.setLevel(Level.INFO);	
		if (emf == null) {
			Map propertyMap = new HashMap();
	        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
			emf = Persistence.createEntityManagerFactory("cassandra_pu", propertyMap);
			logger.debug("\n emf"+ emf.toString());
		}
		return emf;
	}	
	
	public static void main(String[] args) {
		System.out.println("Tacks Counter: "+ CassandraQueryController.getCounterValue("tracks"));
		System.out.println("Looking for user pgaref : found =  "+ CassandraQueryController.findbyEmail("pgaref@example.com").toString());
	}
}