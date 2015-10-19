package main.java.uk.ac.imperial.lsds.play2sdg;

/* SimpleApp.java */
import java.util.Arrays;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SparkSimpleApp {
  public static void main(String[] args) {
    String logFile = "README.md"; // Should be some file on your system
   // String logFile = "hdfs://wombat30.doc.res.ic.ac.uk:8020//user/pg1712/train_triplets.txt";
    
    SparkConf conf = new SparkConf()
    		.setAppName("Eclipse Application")
    		//.set("spark.executor.uri", "hdfs://wombat30.doc.res.ic.ac.uk:8020/spark-1.1.0-bin-2.0.0-cdh4.7.0.tgz")
    		.set("spark.executor.memory","3g")
    		.setMaster("local");
    		//.setMaster("mesos://wombat30.doc.res.ic.ac.uk:5050");
    		//.setJars(new String[]{"/home/pg1712/play2sdg-Spark-module-0.0.1-SNAPSHOT-jar-with-dependencies.jar"});
    
    /*
       val conf = new SparkConf()
          .setMaster("zk://HOSTNAME:2181/mesos")
          .setAppName("My Application")
          .setJars("/path/to/my-application-assembly-1.0.jar" :: Nil)
          .set("spark.executor.uri", "/path/to/spark-1.0.0-bin-hadoop2.tgz")
          .set("spark.mesos.coarse", "true")
        val sc = new SparkContext(conf)

     */
    JavaSparkContext sc = new JavaSparkContext(conf);
    //JavaRDD<String> logData = sc.textFile(logFile).cache();
    JavaRDD<String> logData = sc.textFile(logFile, 50);

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with 'a' : " + numAs + ", lines with 'b' : " + numBs);
  }
}
