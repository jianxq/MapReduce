package info.xiaohei.www.spark;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class NetworkWordCount  {
	 private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
	    SparkConf sparkConf = new SparkConf().setAppName("NetworkWordCount");
	    Duration batchDuration = new Duration(1000);
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchDuration);
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0], Integer.parseInt(args[1]),StorageLevel.MEMORY_AND_DISK_SER());
		JavaPairDStream<String, Integer> counts = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long	serialVersionUID	= 1L;
			@Override
			public Iterable<String> call(String s) throws Exception {
				System.out.println(s);
				return Arrays.asList(SPACE.split(s));
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {
			private static final long	serialVersionUID	= 1L;

			@Override
		    public Tuple2<String, Integer> call(String s) {
		      return new Tuple2<String, Integer>(s, 1);
		    }
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long	serialVersionUID	= 1L;
		    @Override
		    public Integer call(Integer i1, Integer i2) {
		      return i1 + i2;
		    }	
	    });
		System.out.println("启动了");
		counts.print();
		ssc.start();
		ssc.awaitTermination();
		
	
	}
}

