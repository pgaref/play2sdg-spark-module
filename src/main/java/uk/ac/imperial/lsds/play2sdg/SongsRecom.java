package main.java.uk.ac.imperial.lsds.play2sdg;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import main.java.uk.ac.imperial.lsds.models.User;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class SongsRecom {

	
    private transient SparkConf conf;
    /**
     * Main class to calculate Track recommendations for Users
     * The main input is user Playlists 
     * @param conf
     */
    private SongsRecom(SparkConf conf) {
        this.conf = conf;
    }
 
    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        generateData(sc);
    //    compute(sc);
    //    showResults(sc);
        sc.stop();
    }
 
    private void generateData(JavaSparkContext sc) {
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
 
        // Prepare the schema
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS spotify_data");
            session.execute("CREATE KEYSPACE spotify_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE spotify_data.users (email TEXT PRIMARY KEY, username TEXT, password TEXT,firstname TEXT, lastname TEXT)");
            session.execute("CREATE TABLE spotify_data.songs (title TEXT PRIMARY KEY, artist TEXT, releaseDate TIMESTAMP, link TEXT)");
            session.execute("CREATE TABLE spotify_data.playlists (id UUID PRIMARY KEY, folder TEXT, usermail TEXT, titles LIST<TEXT>)");
        }
        
        // Prepare the products hierarchy
        List<User> users = Arrays.asList(
                new User("pangaref@example.com", "pangaref", "secret", "Panagiotis", "Garefalakis"),
                new User("raul@example.com", "raul", "secret", "Raul", "Fernardex"),
                new User("prp@example.com", "prp", "secret", "Peter", "Pietzuch"));
    	
//    
//        // Prepare the products hierarchy
//        List<Product> products = Arrays.asList(
//                new Product(0, "All products", Collections.<Integer>emptyList()),
//                new Product(1, "Product A", Arrays.asList(0)),
//                new Product(4, "Product A1", Arrays.asList(0, 1)),
//                new Product(5, "Product A2", Arrays.asList(0, 1)),
//                new Product(2, "Product B", Arrays.asList(0)),
//                new Product(6, "Product B1", Arrays.asList(0, 2)),
//                new Product(7, "Product B2", Arrays.asList(0, 2)),
//                new Product(3, "Product C", Arrays.asList(0)),
//                new Product(8, "Product C1", Arrays.asList(0, 3)),
//                new Product(9, "Product C2", Arrays.asList(0, 3))
//        );
// 
        JavaRDD<User> usersRDD = sc.parallelize(users);
        javaFunctions(usersRDD).writerBuilder("spotify_data", "users", mapToRow(User.class)).saveToCassandra();
        System.out.println("#### Sucessfuly Persisted 'users' to Cassandra");
        
    }
    
    public static void main(String[] args) {
//      if (args.length != 2) {
//          System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
//          System.exit(1);
//      }

      SparkConf conf = new SparkConf();
      conf.setAppName("Java API demo");
      conf.setMaster("local");
      conf.set("spark.cassandra.connection.host", "localhost");

      SongsRecom app = new SongsRecom(conf);
      app.run();
  }


}
