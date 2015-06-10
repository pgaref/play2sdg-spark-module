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
import main.java.uk.ac.imperial.lsds.models.Recommendations;
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
	
	static Counter songCounter;
	static Counter userCounter;
	
	/**
	 * User - Cassandra JPA
	 * @param user
	 */
	
//	
	public static void persist(User user) {
		EntityManager em = getEmf().createEntityManager();
		User tmp = em.find(User.class, user.getEmail());
		if(tmp == null){
			em.persist(user);
			increment(userCounter, "user");
		}
		else
			em.merge(user);
		em.close();
		logger.debug("\n User: " + user.getEmail() + " record persisted using persistence unit -> cassandra_pu");
	}

	public static User findbyEmail(String email) {
		EntityManager em = getEmf().createEntityManager();
		User user = em.find(User.class, email);
		em.close();
		
		boolean userStr =  ( user == null ? false : true );
		logger.debug("\n Looking for User: " +email + " in cassandra database... found: "
				+ userStr);
		return user;
	}
	
	public static List<User> listAllUsers() {
		EntityManager em = getEmf().createEntityManager();
		Query findQuery = em.createQuery("Select p from User p", User.class);
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
		EntityManager em = getEmf().createEntityManager();
		Track tmp = em.find(Track.class, song.getTrack_id());
		if(tmp == null){
			em.persist(song);
			increment(songCounter, "tracks");
			logger.debug("\n Track: " + song.getTitle() + " record persisted using persistence unit -> " + getEmf().getProperties());
		}
		else{
			em.merge(song);
			logger.debug("\n Track: " + song.getTitle() + " record merged using persistence unit ->" +getEmf().getProperties());
		}
		em.close();
		
	}
	
	public static void remove(Track song) {
		EntityManager em = getEmf().createEntityManager();
		em.remove(song);
		decrement(songCounter, "tracks");
		em.close();
		logger.debug("\n Track: " + song.getTitle() + " record REMOVED using persistence unit ->" +getEmf().getProperties());
	}
	
	
	public static Track findTrackbyTitle(String title) {
		/*
		 * Use NativeQuery to avoid Strings being used as cassandra keywords!
		 * https://github.com/impetus-opensource/Kundera/issues/151
		 * Incompatible with CQL2 !!
		 */
		
		Map propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        EntityManagerFactory tmp = Persistence.createEntityManagerFactory("cassandra_pu", propertyMap);
        EntityManager em  = tmp.createEntityManager();
        
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
		EntityManager em = getEmf().createEntityManager();
		Query findQuery = em.createQuery("Select s from Track s", Track.class);
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
	
	public static void increment(Counter counter, String id) {
		EntityManager em = getEmf().createEntityManager();
		Counter tmp = em.find(Counter.class, id);
		if(tmp == null){
			songCounter = new Counter(id);
			songCounter.incrementCounter();
			em.persist(songCounter);
			logger.debug("Generic: " + songCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
		}
		else{
			songCounter = tmp;
			songCounter.incrementCounter();
			em.merge(songCounter);
			logger.debug("Generic: " + songCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
		}
		em.close();
		
		
	}
	
	public static void decrement(Counter counter, String id) {
		EntityManager em = getEmf().createEntityManager();
		Counter tmp = em.find(Counter.class, id);
		if(tmp == null){
			songCounter = new Counter(id);
			songCounter.decrementCounter();
			em.persist(songCounter);
			logger.debug("Generic: " + songCounter.getId() + "Counter persisted using persistence unit -> cassandra_pu");
		}
		else{
			songCounter = tmp;
			songCounter.decrementCounter();
			em.merge(songCounter);
			logger.debug("Generic: " + songCounter.getId() + "Counter merged using persistence unit -> cassandra_pu");
		}
		em.close();

	}
	
	public static int getCounterValue(String id){
		EntityManager em = getEmf().createEntityManager();
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
	public static void persist(Recommendations r) {
		EntityManager em = getEmf().createEntityManager();
		Recommendations tmp = em.find(Recommendations.class, r.getEmail());
		if(tmp == null){
			em.persist(r);
			logger.debug("\n Recommendations for : " + r.getEmail() + " record persisted using persistence unit -> cassandra_pu");
		}
		else{
			em.merge(r);
			logger.debug("\n Recommendations for : " + r.getEmail() + " record merged using persistence unit -> cassandra_pu");
		}
		em.close();
		
	}
	/**
	 * Playlist - Cassandra JPA
	 * @param Playlist
	 * 
	 */

	public static void persist(PlayList p) {
		EntityManager em = getEmf().createEntityManager();
		PlayList tmp = em.find(PlayList.class, p.getId());
		if(tmp == null){
			em.persist(p);
			logger.debug("\n PlayList: " + p.getId() + " record persisted using persistence unit -> cassandra_pu");
		}
		else{
			em.merge(p);
			logger.debug("\n PlayList: " + p.getId() + " record merged using persistence unit -> cassandra_pu");
		}
		em.close();
		
	}
	
	public static List<PlayList> listAllPlaylists(){
		EntityManager em = getEmf().createEntityManager();
		Query findQuery = em.createQuery("Select p from PlayList p", PlayList.class);
		List<PlayList> allPlaylists= findQuery.getResultList();
		em.close();
		return allPlaylists;
	}

	/**
	 * Entity Manager
	 * @return
	 */
	
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