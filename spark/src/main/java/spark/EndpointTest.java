package spark;

import java.io.FileInputStream;
import java.io.IOException;
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

public class EndpointTest {

	protected static String dbUser = "";
	static String dbHost = "";
	static int dbPort = 0;
	static String dbPwd = "";
	static String mlOrderCollection = "";
	static String mlDbName = "";
	static String productsToPredict;
	static int noRestPort = 0;
	static String endPointModuleName = "";
	static String endPointModulePath = "";
	static String modelPath = "";
	static String includeTraining = "";

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
		noRestPort = Integer.parseInt(prop.getProperty("noRestPort"));
		endPointModuleName = prop.getProperty("endPointModuleName");
		endPointModulePath = prop.getProperty("endPointModulePath");
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
		System.out.println("noRestPort " + noRestPort);
		System.out.println("endPointModuleName " + endPointModuleName);
		System.out.println("endPointModulePath " + endPointModulePath);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("MarkLogicSparkConnector").setMaster("local");
		sparkConf.set("MarkLogic_Host", dbHost);
		sparkConf.set("MarkLogic_Port", prop.getProperty("mlPort"));
		sparkConf.set("MarkLogic_Database", mlDbName);
		sparkConf.set("MarkLogic_User", dbUser);
		sparkConf.set("MarkLogic_Password", dbPwd);

		Operations operations = new Operations();
		List<String> products = operations.tokenizeProducts(productsToPredict);
		SparkSession spark = SparkSession.builder().appName("MarkLogicSparkConnector").config(sparkConf).getOrCreate();

		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		System.out.println("Spawning " + Runtime.getRuntime().availableProcessors() + " threads...");

		for (int iCounter = 0; iCounter < products.size(); iCounter++) {
			executor.execute(
					new MyEndpointPredictions(iCounter, spark, products.get(iCounter), modelPath, includeTraining));
		}
		executor.awaitTermination(1, TimeUnit.HOURS);
		executor.shutdown(); // once you are done with ExecutorService
		spark.stop();
	}
}

class MyEndpointPredictions implements Runnable {
	int id;
	SparkSession spark;
	String productName;
	DatabaseClient restClient;
	DatabaseClient noRestClient;
	String endPointModulePath;
	String endPointModuleName;
	String modelPath;
	String includeTraining;

	public MyEndpointPredictions(int i, SparkSession spark, String productName, String modelPath,
			String includeTraining) {
		this.id = i;
		this.spark = spark;
		this.productName = productName;
		this.noRestClient = DatabaseClientFactory.newClient(EndpointTest.dbHost, EndpointTest.noRestPort,
				new DatabaseClientFactory.DigestAuthContext(EndpointTest.dbUser, EndpointTest.dbPwd));
		this.restClient = DatabaseClientFactory.newClient(EndpointTest.dbHost, EndpointTest.dbPort,
				EndpointTest.mlDbName,
				new DatabaseClientFactory.DigestAuthContext(EndpointTest.dbUser, EndpointTest.dbPwd));
		this.endPointModuleName = EndpointTest.endPointModuleName;
		this.endPointModulePath = EndpointTest.endPointModulePath;
		this.modelPath = EndpointTest.modelPath;
		this.includeTraining = EndpointTest.includeTraining;
	}

	public void run() {
		try {
			Operations operations = new Operations();
			System.out.println(
					"Prediction for '" + productName + "' started by Thread " + Thread.currentThread().getName());
			operations.performEndpointPredictions(spark, noRestClient, restClient, productName, endPointModuleName,
					endPointModulePath, modelPath, includeTraining);
			System.out.println(
					"Prediction for '" + productName + "' ended by Thread " + Thread.currentThread().getName());
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}
