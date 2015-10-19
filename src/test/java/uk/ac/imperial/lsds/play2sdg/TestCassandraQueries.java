package test.java.uk.ac.imperial.lsds.play2sdg;

import java.util.Date;
import java.util.List;

import main.java.uk.ac.imperial.lsds.cassandra.KunderaCassandraQueryController;
import main.java.uk.ac.imperial.lsds.models.PlayList;
import main.java.uk.ac.imperial.lsds.models.Recommendation;
import main.java.uk.ac.imperial.lsds.models.Stats;
import main.java.uk.ac.imperial.lsds.models.StatsTimeseries;
import main.java.uk.ac.imperial.lsds.models.Track;
import main.java.uk.ac.imperial.lsds.models.User;
import main.java.uk.ac.imperial.lsds.utils.SystemStats;

import org.apache.log4j.PropertyConfigurator;

public class TestCassandraQueries {
	
	
	public static void TestListUsers(){
		List<User> list = KunderaCassandraQueryController.listAllUsers();
		assert(list != null);
		assert(list.size() != 0);
		assert(list.get(0) != null);	
		System.out.println("Got -> "+ list.size() + " Users");
	}
	
	public static void TestListSongs(){
		List<Track> list = KunderaCassandraQueryController.listAllTracksWithPagination();
		assert(list != null);
		assert(list.size() != 0);
		assert(list.get(0) != null);
		System.out.println("Got -> "+ list.size() + " Tracks");
	}
	
	public static void testListRecommendations(){
		List<Recommendation> list = KunderaCassandraQueryController.listAllRecommendations();
		assert(list != null);
		assert(list.size() != 0);
		assert(list.get(0) != null);
		System.out.println("Got -> "+ list.size() + " Recommendation");
		//Check init data corectness
		assert( KunderaCassandraQueryController.getUserRecc("pgaref@example.com") != null );
	}
	
	public static void testAddRec(){
		Recommendation r = new Recommendation("pgaref@example.com");
		r.addRecommendation("DasdsadsaXXX", 5.0);
		r.addRecommendation("anotherOne", 4.0);
		KunderaCassandraQueryController.persist(r);
	}
	
	public static void TestAddUserPlayList(){
		PlayList p = new PlayList("pgaref@example.com", "whatever");
		p.addRatingSong(KunderaCassandraQueryController.listAllTracks().get(0));
		KunderaCassandraQueryController.persist(p);
		
		p.addRatingSong(KunderaCassandraQueryController.listAllTracks().get(1));
		KunderaCassandraQueryController.persist(p);
		
	}
	
	
	public static void testStats(){
		Stats s  = new Stats("testStat");
		s.getStatsMap().put("performance", 1.0);
		s.getStatsMap().put("memory", (double)2000);
		s.getStatsMap().put("errors", (double) 0);
		s.setTimestamp(new Date());
		KunderaCassandraQueryController.persist(s);
		
		assert(KunderaCassandraQueryController.getAllStats() == null);
		assert(KunderaCassandraQueryController.getAllStats().size() < 1 );
		
		for(Stats tmp : KunderaCassandraQueryController.getAllStats()){
			System.out.println("Got statistic: "+ tmp.getId() );
			System.out.println("With Timestamp: "+ tmp.getTimestamp());
			System.out.println("with Values: "+ tmp.getStatsMap());
		}
	}
	
	
	public static void testTimeseriesStats(){
		
		SystemStats perf  = new SystemStats();
		StatsTimeseries ts = new StatsTimeseries("Spark-statseries");
		ts.setTimestamp(new Date());
		ts.getStatsMap().put("os-name", perf.getOsName());
		ts.getStatsMap().put("cpu-vendor", perf.getCpuVendor());
		ts.getStatsMap().put("cpu-freq", perf.getCpuFreq()+"");
		ts.getStatsMap().put("cores-num", perf.getCpuCores()+"");
		ts.getStatsMap().put("system-load", perf.getSystemLoad()+"");
		ts.getStatsMap().put("system-loadavg", perf.getSystemLoadAverage() +"");
		
		int num = 0;
		for(Double val: perf.getCoresLoad()){
			ts.getStatsMap().put("core-"+num, val+"");
			num++;
		}
		
		ts.getStatsMap().put("mem-total", perf.getMemTotal()+"");
		ts.getStatsMap().put("mem-avail", perf.getMemAvailable()+"");
		
		KunderaCassandraQueryController.persist(ts);
		
		List<StatsTimeseries> l = KunderaCassandraQueryController.getAllStatsTimeseries("Spark-statseries");
		for(StatsTimeseries t : l ){
			System.out.println("Read StatTs "+ t);
		}
	}
	
	public static void testDeletePlayList(){
		List<PlayList> found  =  (List<PlayList>) KunderaCassandraQueryController.getUserPlayLists("pgaref@example.com");
		boolean result = KunderaCassandraQueryController.deleteUserPlayListSong(found.get(0).getId(), "Yunu Yucu Ninu");
		System.out.println("Delete query result: "+ result);
	}
	
	public static void main(String[] args) {
		/*
		 * TODO: Change to load Data just for the tests!
		 */
		PropertyConfigurator.configure("conf/META-INF/log4j.properties");
		
	/*		
		TestListUsers();
		TestListSongs();
		testListRecommendations();
		
	*/
//		for(int i = 0 ; i < 2000; i++)
//			testAddRec();
		
//		TestAddUserPlayList();
		
//		testStats();
		testTimeseriesStats();

		
//		testDeletePlayList();
		
	}

}
