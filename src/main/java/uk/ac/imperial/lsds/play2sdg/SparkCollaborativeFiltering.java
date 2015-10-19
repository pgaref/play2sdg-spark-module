package main.java.uk.ac.imperial.lsds.play2sdg;


import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

import main.java.uk.ac.imperial.lsds.dx_controller.CassandraDxQueryController;
import main.java.uk.ac.imperial.lsds.dx_controller.ClusterManager;
import main.java.uk.ac.imperial.lsds.dx_models.PlayList;
import main.java.uk.ac.imperial.lsds.dx_models.Recommendation;
import main.java.uk.ac.imperial.lsds.dx_models.StatsTimeseries;
import main.java.uk.ac.imperial.lsds.dx_models.Track;
import main.java.uk.ac.imperial.lsds.dx_models.User;
import main.java.uk.ac.imperial.lsds.io_handlers.RatingsFileWriter;
import main.java.uk.ac.imperial.lsds.utils.SystemStats;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import com.datastax.spark.connector.japi.CassandraRow;


public class SparkCollaborativeFiltering implements Serializable{

	static Logger logger = Logger.getLogger(SparkCollaborativeFiltering.class);
	
	/**
	 * Change for HDFS
	 */
	//private static final String dataset_path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/spark-data";
	//private static final String dataset_path = "/spark-data";
	
	private static List<PlayList> allplaylists;
	private static Map<String, Integer> usersMap;
	private static List<User> allusers;
	private static Map<String, Integer> tracksMap;
	private static List<String> tracksList;
//	private static ClusterManager clusterManager = new ClusterManager("play_cassandra", 1, "146.179.131.141");
//	private static CassandraDxQueryController dxController = new CassandraDxQueryController(clusterManager.getSession());
	
	//No need to serialise
	private transient SparkConf conf;
	public SparkCollaborativeFiltering(SparkConf c) {this.conf = c;}
	
	public static SparkConf createSparkConf(String master, String cassandra_host){
    	return new SparkConf()
    	.setAppName("Spark Cassandra Connector DEMO")
    	.setMaster(master)
    	.set("spark.cassandra.connection.host", cassandra_host)
    	//Not-Compatible in driver version 1.2
    	//.set("spark.cassandra.input.split.size_in_mb", "67108864")
    	.set("spark.executor.memory", "1g");
    }
		
	static{
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		//This is the root logger provided by log4j
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.INFO);
		java.util.logging.Logger.getGlobal().setLevel(java.util.logging.Level.INFO);
	}
	
	private void run() {
		long startTime = System.currentTimeMillis();
		SparkCassandraConnector cassandraConnector = new SparkCassandraConnector(this.conf);
		JavaSparkContext sc = new JavaSparkContext(conf);
		/*
		 *  Fetch the Track List 
		*/ 
		logger.info("## Listing all Tracks Stored at Cassandra ## ");
		tracksList = cassandraConnector.fetchAllTracks(sc);
		
		tracksMap = generateTrackMap( );
		logger.info("## Generated # "+ tracksMap.size() +" Track IDs ##");
		
		allplaylists = cassandraConnector.fetchAllPlayLists(sc);
		allusers = cassandraConnector.fetchAllUsers(sc);
		
		usersMap = generateUserMap( );
		logger.info("## Generated # "+ usersMap.size() +" User IDs ##");
		
		for(String tmp : usersMap.keySet())
			System.out.println("UserMap K:"+tmp + " V: "+usersMap.get(tmp));
		
		
		JavaRDD<Rating> ratings = generateRatings(sc);
		logger.info("## Generated # "+ ratings.count() +" Ratings ##");
		/*
		ALSRecommendationModel alsRec = new ALSRecommendationModel();
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = alsRec.runModel(ratings);
		cassandraConnector.persistPredictions(sc, allusers, tracksList, predictions);
		cassandraConnector.persistStatData(sc, startTime, alsRec.getPredictionsSize(), alsRec.getMSE());
		*/
		logger.info("Spark job Finished!");
	}
	

	
	public static void main(String[] args) {
		
		String spark_host= null;
		String cassandra_host = null;
		
		if (args.length == 2) {
			spark_host = args[0];
			cassandra_host=args[1];
		}
		else if(args.length != 0){
			System.err.println("Syntax: main.java.uk.ac.imperial.lsds.play2sdg.SparkCollaborativeFiltering <Spark Master URL> <Cassandra contact point>");
			System.exit(1);
		}
		
		SparkConf conf;
		if((spark_host==null) && (cassandra_host==null))
			conf = createSparkConf("local[8]", "wombat11.doc.res.ic.ac.uk");
		else
			conf = createSparkConf(spark_host, cassandra_host);
			
		
		SparkCollaborativeFiltering cf = new SparkCollaborativeFiltering(conf);
		cf.run();

	}
	
	
	private int trackID = 0;
	private int userID = 0;
	
	private Map<String, Integer>  generateUserMap(){
		Map<String , Integer> m = new HashMap<String , Integer>();
		userID = 0;
		for(User u : allusers){
			m.put(u.getEmail(), userID);
			userID++;
		}
		return m;		
	}

	private Map<String, Integer>  generateTrackMap(){
		Map<String, Integer> m = new HashMap<String, Integer>(tracksList.size());
		
		for(trackID=0; trackID < tracksList.size(); trackID++){
			String title = tracksList.get(trackID);
			System.out.println("TrackTitle "+ title +"Track ID " +trackID);
			m.put(title, trackID);
		}
		
		return m;
	}

	private JavaRDD<Rating> generateRatings(JavaSparkContext sc){
		List<String> ratingList = new ArrayList<String>();
 		/*
		 * Convert IDS and save to HDFS File
		 */
		System.out.println("PL SIze: " +allplaylists.size());
		for(PlayList playList : allplaylists){
			System.out.println("Pl Tracks Size " +playList.getTracks().size() );
			System.out.println("Pl Tracks " +playList.getTracks().toString() );
			for(String track : playList.getTracks()){
				StringBuilder sb = new StringBuilder();
				sb.append(usersMap.get(playList.getUsermail()) + ",");
				sb.append(tracksMap.get(track) + ",");
				sb.append("5.0");
				ratingList.add(sb.toString());
			}
		}
		logger.info("## Converted ratings from: "+allplaylists.size() + " playlists##");
		System.out.println("New Ratings List: "+ ratingList.size());
		
		for(String tmp : ratingList)
			System.out.println("Rating "+ tmp);
		
		/*
		 * Persist To HDFS
		 * 
			RatingsFileWriter rw = new RatingsFileWriter(dataset_path);
			rw.persistRatingsFile(ratingList);
			// Load and parse the data
			String path = dataset_path +"/ratings.data";
		logger.info("## Persisting to HDFS -> Done ##");
		*/
				
		JavaRDD<String> data = sc.parallelize(ratingList);
		JavaRDD<Rating> ratings = data.map(new Function<String, Rating>() {
			public Rating call(String s) {
				String[] sarray = s.split(",");
				return new Rating(Integer.parseInt(sarray[0]), Integer
						.parseInt(sarray[1]), Double.parseDouble(sarray[2]));
			}
		}).cache();
		
		return ratings;
	}
	
	/**
	 * Method Mapping generated Recommendations to Tracks and Users 
	 * @param predictions
	 * 
	public static void MapPredictions2Tracks(JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions){
		for( Tuple2 <Tuple2<Integer, Integer>,Double> pred: predictions.toArray() ){
			logger.debug("Creating Recommendation-> user: "+pred._1()._1 + "\t track: " + pred._1()._2 + "\t score: "+pred._2() );
			Recommendation newRec = new Recommendation(allusers.get(pred._1()._1).getEmail());
			newRec.getRecMap().put(tracksList.get(pred._1()._2), pred._2());
			dxController.persist(newRec);
		}
	}*/
}
