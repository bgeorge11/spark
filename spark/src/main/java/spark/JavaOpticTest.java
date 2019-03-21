package spark;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

public class JavaOpticTest {

	protected static String dbUser = "";
	static String dbHost = "";
	static int dbPort = 0;
	static String dbPwd = "";
	static String mlOrderCollection = "";
	static String mlDbName = "";
	static String productsToPredict;
	static String modelPath;
	static String includeTraining;

	public static void main(String[] args) throws IOException, InterruptedException {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getRootLogger().setLevel(Level.ERROR);

		String fileName = args[0];
		Properties prop = new Properties();
		FileInputStream input = null;
		input = new FileInputStream(fileName);
		System.out.println("Loading properties... " + fileName);
		prop.load(input);

		dbUser = prop.getProperty("mlUser");
		dbHost = prop.getProperty("mlHost");
		dbPort = Integer.parseInt(prop.getProperty("mlPort"));
		dbPwd = prop.getProperty("mlPwd");
		mlOrderCollection = prop.getProperty("mlOrderCollection");
		mlDbName = prop.getProperty("mlDbName");
		productsToPredict = prop.getProperty("productsToPredict");
		modelPath = prop.getProperty("pathToStoreModel");
		includeTraining = prop.getProperty("includeTraining");

		System.out.println("***** Properties ****");
		System.out.println("mlUser " + dbUser);
		System.out.println("mlHost " + dbHost);
		System.out.println("mlPort " + dbPort);
		System.out.println("mlPwd " + dbPwd);
		System.out.println("mlOrderCollection " + mlOrderCollection);
		System.out.println("mlDbName " + mlDbName);
		System.out.println("productsToPredict " + productsToPredict);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("MarkLogicSparkConnector").setMaster("local");
		sparkConf.set("MarkLogic_Host", dbHost);
		sparkConf.set("MarkLogic_Port", prop.getProperty("mlPort"));
		sparkConf.set("MarkLogic_Database", mlDbName);
		sparkConf.set("MarkLogic_User", dbUser);
		sparkConf.set("MarkLogic_Password", dbPwd);

		Operations operations = new Operations();
		JavaConnectorOperations mlOperations = new JavaConnectorOperations();
		List<String> products = operations.tokenizeProducts(productsToPredict);
		SparkSession spark = SparkSession.builder().appName("MarkLogicSparkConnector").config(sparkConf).getOrCreate();

		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		System.out.println("Spawning " + Runtime.getRuntime().availableProcessors() + " threads...");

		for (int iCounter = 0; iCounter < products.size(); iCounter++) {
			executor.execute(
					new MyProductLevelPredictions(iCounter, spark, products.get(iCounter), modelPath, includeTraining));
		}
		executor.awaitTermination(20, TimeUnit.MINUTES);
		executor.shutdown(); // once you are done with ExecutorService
		spark.stop();
	}
}

class MyProductLevelPredictions implements Runnable {
	int id;
	SparkSession spark;
	String productName;
	DatabaseClient client;
	String modelPath;
	String includeTraining;

	public MyProductLevelPredictions(int i, SparkSession spark, String productName, String modelPath,
			String includeTraining) {
		this.id = i;
		this.spark = spark;
		this.productName = productName;
		this.modelPath = modelPath;
		this.client = DatabaseClientFactory.newClient(JavaOpticTest.dbHost, JavaOpticTest.dbPort,
				JavaOpticTest.mlDbName,
				new DatabaseClientFactory.DigestAuthContext(JavaOpticTest.dbUser, JavaOpticTest.dbPwd));
		this.includeTraining = includeTraining;
	}

	public void run() {
		Operations operations = new Operations();
		Date start = new Date();
		try {

			System.out.println(
					"Prediction for '" + productName + "' started by Thread " + Thread.currentThread().getName());
			operations.performOpticPrediction(spark, client, productName, modelPath, includeTraining);
			Date end = new Date();
			System.out
			.println("Prediction for " + productName + " ended by Thread " + Thread.currentThread().getName() + " in  " + (end.getTime() - start.getTime()) / 1000 + " seconds.");
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}
