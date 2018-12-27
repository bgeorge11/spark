package spark;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.spark.NiFiDataPacket;
import org.apache.nifi.spark.NiFiReceiver;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
/* SimpleApp.java */
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class OLDSparkStreamingApp {
	public static List<String> keywords = new ArrayList<String>();
	public static ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
			.enable(SerializationFeature.INDENT_OUTPUT).disable(DeserializationFeature.UNWRAP_ROOT_VALUE);
  private static final AtomicLong runningSum = new AtomicLong(0);
  private static final AtomicLong runningCount = new AtomicLong(0);
  private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

	public static void main(String[] args) throws InterruptedException, IOException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getRootLogger().setLevel(Level.ERROR);
		
		String fileName = args[0];
		Properties prop = new Properties();
		FileInputStream input = null;
		input = new FileInputStream(fileName);
		System.out.println("Loading properties... " + fileName);
		prop.load(input);
		String checkPointDir = prop.getProperty("checkPointDir");
		String windowDuration = prop.getProperty("windowDuration");
		String slideDuration = prop.getProperty("slideDuration");
		String nifiHost = prop.getProperty("nifiHost");
		
		SparkSession spark = SparkSession.builder().appName("NiFi-Spark Streaming example").getOrCreate();
		JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());
		JavaStreamingContext ssc = new JavaStreamingContext(ctx, new Duration(1000L));
		ssc.checkpoint(checkPointDir);
		SiteToSiteClientConfig config = new SiteToSiteClient.Builder().url(nifiHost)
				.portName("Data For Spark").buildConfig();
		JavaReceiverInputDStream<?> packetStream = ssc
				.receiverStream(new NiFiReceiver(config, StorageLevel.MEMORY_ONLY()));
		JavaDStream<String> content = packetStream
				.map(dataPacket -> new String(((NiFiDataPacket) dataPacket).getContent(), StandardCharsets.UTF_8));
		//content.print();
		
		List<Order> lstOrder = new ArrayList<Order>();
		JavaDStream<List<Order>> orderStream = content.map(new Function<String, List<Order>>() {
			@Override
			public List<Order> call(String x) throws JsonParseException, JsonMappingException, IOException {
				Order order = mapper.readValue(x, Order.class);
				lstOrder.add(order);
				return lstOrder;
			}
		});
      JavaDStream<Long> totalOrders = content.countByWindow(Duration.apply(Long.parseLong(windowDuration)), 
    		  												Duration.apply(Long.parseLong(slideDuration)));
      totalOrders.foreachRDD(rdd -> {
          if (rdd.count() > 0) {
            runningSum.getAndAdd(rdd.reduce(SUM_REDUCER));
            runningCount.getAndAdd(rdd.count());
            System.out.print("Running Count of Orders is " + runningSum.get() + "\n");
          }
          return;
        });
		ssc.start();
		ssc.awaitTermination();
	}
}