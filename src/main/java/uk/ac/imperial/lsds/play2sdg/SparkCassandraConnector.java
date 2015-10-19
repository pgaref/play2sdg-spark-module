package main.java.uk.ac.imperial.lsds.play2sdg;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.column;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import main.java.uk.ac.imperial.lsds.dx_models.PlayList;
import main.java.uk.ac.imperial.lsds.dx_models.Recommendation;
import main.java.uk.ac.imperial.lsds.dx_models.StatsTimeseries;
import main.java.uk.ac.imperial.lsds.dx_models.Track;
import main.java.uk.ac.imperial.lsds.dx_models.User;
import main.java.uk.ac.imperial.lsds.utils.SystemStats;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


public class SparkCassandraConnector implements Serializable {

	static Logger logger = Logger.getLogger(SparkCassandraConnector.class);
    private transient SparkConf conf;
    /**
     * Main class to calculate Track recommendations for Users
     * The main input is user Playlists 
     * @param conf
     */
    public SparkCassandraConnector(SparkConf conf) {
        this.conf = conf;
    }
 
    
    public List<String> fetchAllTracks(JavaSparkContext sc){
    	JavaRDD<String> cassandraRowsRDD = javaFunctions(sc).cassandraTable("play_cassandra", "tracks").select("title")
    	        .map(new Function<CassandraRow, String>() {
    	            @Override
    	            public String call(CassandraRow cassandraRow) throws Exception {
    	                return cassandraRow.getString("title");
    	            }
    	        }).persist(StorageLevel.MEMORY_AND_DISK()	);
    	//System.out.println("Data Read as CassandraRows: \n" + StringUtils.join(cassandraRowsRDD.toArray(), "\n"));
    	logger.info("Fetched Tracks Data size: "+ (double)(sizeOf(cassandraRowsRDD.toArray()).length/1024)/1024 +" MBytes " + cassandraRowsRDD.toArray().size() +" elements");
    	return cassandraRowsRDD.toArray();
    }
    
    public List<PlayList> fetchAllPlayLists(JavaSparkContext sc){
    	CassandraJavaRDD<PlayList> rdd = javaFunctions(sc).cassandraTable("play_cassandra", "playlists", mapRowTo(PlayList.class))
    			.select( "id", "usermail", "folder", "tracks");
    	logger.info("Fetched PlayLists Data size : "+ (double)(sizeOf(rdd.toArray()).length/1024)/1024 +" MBytes " + rdd.toArray().size() +" elements" );
    	return rdd.toArray();
    }
    
    public List<User> fetchAllUsers(JavaSparkContext sc){
    	CassandraJavaRDD<User> rdd = javaFunctions(sc).cassandraTable("play_cassandra", "users", mapRowTo(User.class))
    			.select( 
    					column("key").as("email"),
    					column("username"),
    					column("password"),
    					column("firstname").as("fistname"),
    					column("lastname")
    					);
    	logger.info("Fetched User Data size : "+ (double)(sizeOf(rdd.toArray()).length/1024)/1024 +" MBytes " + rdd.toArray().size() +" elements" );
    	return rdd.toArray();
    }
    
    public void persistPredictions(JavaSparkContext sc, List<User> allusers, List<String> tracksList, JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions){
		
		/**
		 * Create recommendations based on stored Track and User id
		 * Similar Implementation with -> MapPredictions2Tracks(predictions) method
		 */
		List<Recommendation> newUserSongRec = new ArrayList<Recommendation>();
		for( Tuple2 <Tuple2<Integer, Integer>,Double> pred: predictions.toArray() ){
			logger.debug("Creating Recommendation-> user: "+pred._1()._1 + "\t track: " + pred._1()._2 + "\t score: "+pred._2() );
			Recommendation newRec = new Recommendation(allusers.get(pred._1()._1).getEmail());
			newRec.getRecMap().put(tracksList.get(pred._1()._2), pred._2());
			newUserSongRec.add(newRec);
			//CassandraQueryController.persist(newRec);
		}

		/**
		 * Create an RDD from recommendations and Save it in parallel fashion
		 */
		JavaRDD<Recommendation> rdd = sc.parallelize(newUserSongRec);
		rdd.persist(StorageLevel.MEMORY_AND_DISK_SER());
		
		CassandraJavaUtil.javaFunctions(rdd)
        	.writerBuilder("play_cassandra", "recommendations ", mapToRow(Recommendation.class)).saveToCassandra();
//		rdd.foreach(new VoidFunction<Recommendation>() {
//			@Override
//			public void call(Recommendation r) throws Exception {
//				dxController.persist(r);
//			}
//		});
		
    }
    
    public void persistStatData(JavaSparkContext sc, long jobStarted, long predictionCount, double MSE ){
		/*
		 * Update Stats Table 
 		 */
		SystemStats perf  = new SystemStats();
		StatsTimeseries sparkJobStats = new StatsTimeseries("sparkCF");
		sparkJobStats.getMetricsMap().put("Job time(s)", ((System.currentTimeMillis()-jobStarted)/1000)+"" ); 
		sparkJobStats.getMetricsMap().put("Total Predictions", predictionCount+"" );
		sparkJobStats.getMetricsMap().put("Mean Squared Error",  MSE+"" );
		
		//-> Added Performance Data
		sparkJobStats.collectData(perf);
		
		List<StatsTimeseries> allStats = new ArrayList<StatsTimeseries>();
		allStats.add(sparkJobStats);
		
		JavaRDD<StatsTimeseries> rdd = sc.parallelize(allStats);
		CassandraJavaUtil.javaFunctions(rdd)
    		.writerBuilder("play_cassandra", "statseries ", mapToRow(StatsTimeseries.class)).saveToCassandra();
		
		logger.info("Finished Writing new User-Song Predictions - Cassandra-Spark Connector - Job took: "+Double.parseDouble( ((System.currentTimeMillis()-jobStarted)/1000)+""));
		
    }
    
	private static byte[] sizeOf(Object obj) {
		ByteArrayOutputStream byteObject = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream;
		try {
			objectOutputStream = new ObjectOutputStream(byteObject);
			objectOutputStream.writeObject(obj);
			objectOutputStream.flush();
			objectOutputStream.close();
			byteObject.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return byteObject.toByteArray();
	}
    
    /*
     * For Testing only!
     */
    public static void main(String[] args) {
      SparkConf conf = SparkCollaborativeFiltering.createSparkConf("local[8]", "wombat11.doc.res.ic.ac.uk");
      
      
      long start = System.currentTimeMillis();
      
      SparkCassandraConnector app = new SparkCassandraConnector(conf);
      JavaSparkContext sc = new JavaSparkContext(conf);
      app.fetchAllTracks(sc);
      //app.fetchAllUsers(sc);
      
      System.out.println("Job took "+ ((double) (System.currentTimeMillis()-start)/1000) + " seconds");
  }


}
