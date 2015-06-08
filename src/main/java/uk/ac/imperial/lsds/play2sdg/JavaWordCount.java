package main.java.uk.ac.imperial.lsds.play2sdg;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class JavaWordCount {
	
	private static final Pattern SPACE = Pattern.compile(" ");
	//public static String inputFile = "data/songs/train_triplets.txt";
	public static String inputFile = "hdfs://wombat30.doc.res.ic.ac.uk:8020//user/pg1712/train_triplets.txt";
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
					/*.set("spark.executor.memory","1g")
					.setAppName("Spark Java WordCount")
					.setMaster("local");*/
					.setAppName("Distributed WordCount")
					.set("spark.executor.uri", "hdfs://wombat30.doc.res.ic.ac.uk:8020/spark-1.1.0-bin-2.0.0-cdh4.7.0.tgz")
				//	.set("spark.executor.memory","4g")
					//.set("spark.storage.memoryFraction", "0.7")
					.setMaster("mesos://wombat30.doc.res.ic.ac.uk:5050");
		
		JavaSparkContext ctx = new JavaSparkContext(conf);
		//For one file
		/*
		 * If you get Kryo Decompression Exceptions try increasing the partitions number
		 */
	    JavaRDD<String> lines = ctx.textFile(inputFile, 100);

	    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	      @Override
	      public Iterable<String> call(String s) {
	        return Arrays.asList(SPACE.split(s));
	      }
	    });

	    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
	      @Override
	      public Tuple2<String, Integer> call(String s) {
	        return new Tuple2<String, Integer>(s, 1);
	      }
	    });

	    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
	      @Override
	      public Integer call(Integer i1, Integer i2) {
	        return i1 + i2;
	      }
	    });

	    List<Tuple2<String, Integer>> output = counts.collect();
	    for (Tuple2<?,?> tuple : output) {
	      System.out.println(tuple._1() + ": " + tuple._2());
	    }
	    ctx.stop();
	  }

//		//most popular words threshold
//		final int threshold = 2;
//
//		// split each document into words
//		JavaRDD<String> tokenized = sc.textFile(inputFile).flatMap(
//				new FlatMapFunction<String, String>() {
//					@Override
//					public Iterable<String> call(String s) {
//						return Arrays.asList(s.split(" "));
//					}
//				});
//
//		// count the occurrence of each word
//		JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
//				new PairFunction<String, String, Integer>() {
//					@Override
//					public Tuple2<String, Integer> call(String s) {
//						return new Tuple2<String, Integer>(s, 1);
//					}
//				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
//			@Override
//			public Integer call(Integer i1, Integer i2) {
//				return i1 + i2;
//			}
//		});
//
//		// filter out words with less than threshold occurrences
//		JavaPairRDD<String, Integer> filtered = counts
//				.filter(new Function<Tuple2<String, Integer>, Boolean>() {
//					@Override
//					public Boolean call(Tuple2<String, Integer> tup) {
//						return tup._2() >= threshold;
//					}
//				});
//		System.out.println(filtered.collect());
//
//		// count characters
//		JavaPairRDD<Character, Integer> charCounts = filtered
//				.flatMap(
//						new FlatMapFunction<Tuple2<String, Integer>, Character>() {
//							@Override
//							public Iterable<Character> call(
//									Tuple2<String, Integer> s) {
//								Collection<Character> chars = new ArrayList<Character>(
//										s._1().length());
//								for (char c : s._1().toCharArray()) {
//									chars.add(c);
//								}
//								return chars;
//							}
//						})
//				.mapToPair(new PairFunction<Character, Character, Integer>() {
//					@Override
//					public Tuple2<Character, Integer> call(Character c) {
//						return new Tuple2<Character, Integer>(c, 1);
//					}
//				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
//					@Override
//					public Integer call(Integer i1, Integer i2) {
//						return i1 + i2;
//					}
//				});
//
//		System.out.println(charCounts.collect());
//	}

}
