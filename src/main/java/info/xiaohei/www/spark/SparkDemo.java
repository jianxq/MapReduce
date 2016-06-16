package info.xiaohei.www.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkDemo {
	
	private static Logger log = LoggerFactory.getLogger(SparkDemo.class);
	
	public static void main(String[] args) {
		String logFile = "/examp/wordcount/input/1.txt"; // Should be some file on your system
	    SparkConf conf = new SparkConf().setAppName("Simple Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> logData = sc.textFile(logFile).cache();
	    long numAs = logData.filter((s)->{
	    	System.out.println(s);
	    	return s.contains("a");
	    }).count();
	    long numBs = logData.filter((s)->s.contains("b")).count();
	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	    logData.foreach((s)->log.info(s));
	  }
}
