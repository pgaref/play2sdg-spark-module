package test.java.uk.ac.imperial.lsds.play2sdg;

import java.util.List;

import main.java.uk.ac.imperial.lsds.cassandra.CassandraQueryController;
import main.java.uk.ac.imperial.lsds.models.PlayList;
import main.java.uk.ac.imperial.lsds.models.Recommendation;
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
		List<Track> list = CassandraQueryController.listAllTracks();
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
	
	
	public static void main(String[] args) {
		/*
		 * TODO: Change to load Data just for the tests!
		 */
		
		TestListUsers();
		TestListSongs();
		testListRecommendations();
		
		testAddRec();
		TestAddUserPlayList();
		
	}

}
