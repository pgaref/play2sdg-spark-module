package test.java.uk.ac.imperial.lsds.play2sdg;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import main.java.uk.ac.imperial.lsds.cassandra.CassandraQueryController;
import main.java.uk.ac.imperial.lsds.models.PlayList;
import main.java.uk.ac.imperial.lsds.models.Recommendation;
import main.java.uk.ac.imperial.lsds.models.Stats;
import main.java.uk.ac.imperial.lsds.models.Track;
import main.java.uk.ac.imperial.lsds.models.User;

public class TestCassandraQueries {
	
	
	public static void TestListUsers(){
		List<User> list = CassandraQueryController.listAllUsers();
		assert(list != null);
		assert(list.size() != 0);
		assert(list.get(0) != null);	
		System.out.println("Got -> "+ list.size() + " Users");
	}
	
	public static void TestListSongs(){
		List<Track> list = CassandraQueryController.listAllTracksWithPagination();
		assert(list != null);
		assert(list.size() != 0);
		assert(list.get(0) != null);
		System.out.println("Got -> "+ list.size() + " Tracks");
	}
	
	public static void testListRecommendations(){
		List<Recommendation> list = CassandraQueryController.listAllRecommendations();
		assert(list != null);
		assert(list.size() != 0);
		assert(list.get(0) != null);
		System.out.println("Got -> "+ list.size() + " Recommendation");
		//Check init data corectness
		assert( CassandraQueryController.getUserRecc("pgaref@example.com") != null );
	}
	
	public static void testAddRec(){
		Recommendation r = new Recommendation("pgaref@example.com");
		r.addRecommendation("DasdsadsaXXX", 5.0);
		r.addRecommendation("anotherOne", 4.0);
		CassandraQueryController.persist(r);
	}
	
	public static void TestAddUserPlayList(){
		PlayList p = new PlayList("pgaref@example.com", "whatever");
		p.addRatingSong(CassandraQueryController.listAllTracks().get(0));
		CassandraQueryController.persist(p);
		
		p.addRatingSong(CassandraQueryController.listAllTracks().get(1));
		CassandraQueryController.persist(p);
		
	}
	
	
	public static void testStats(){
		Stats s  = new Stats("testStat");
		s.getStatsMap().put("performance", 1.0);
		s.getStatsMap().put("memory", (double)2000);
		s.getStatsMap().put("errors", (double) 0);
		s.setTimestamp(new Date());
		CassandraQueryController.persist(s);
		
		assert(CassandraQueryController.getAllStats() == null);
		assert(CassandraQueryController.getAllStats().size() < 1 );
		
		for(Stats tmp : CassandraQueryController.getAllStats()){
			System.out.println("Got statistic: "+ tmp.getId() );
			System.out.println("With Timestamp: "+ tmp.getTimestamp());
			System.out.println("with Values: "+ tmp.getStatsMap());
		}
	}
	
	
	public static void testDeletePlayList(){
		List<PlayList> found  =  (List<PlayList>) CassandraQueryController.getUserPlayLists("pgaref@example.com");
		boolean result = CassandraQueryController.deleteUserPlayListSong(found.get(0).getId(), "Yunu Yucu Ninu");
		System.out.println("Delete query result: "+ result);
	}
	
	public static void main(String[] args) {
		/*
		 * TODO: Change to load Data just for the tests!
		 */
		PropertyConfigurator.configure("conf/META-INF/log4j.properties");
		
		TestListUsers();
		TestListSongs();
		testListRecommendations();
		
		for(int i = 0 ; i < 2000; i++)
			testAddRec();
		
//		TestAddUserPlayList();
		
		testStats();
//		testDeletePlayList();
		
	}

}
