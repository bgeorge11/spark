package spark;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

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
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

public class ScalaConnectorTest {

	protected static String dbUser = "";
	static String dbHost = "";
	static int dbPort = 0;
	static String dbPwd = "";
	static String mlOrderCollection = "";
	static String mlDbName = "";
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
		modelPath = prop.getProperty("pathToStoreModel");
		includeTraining = prop.getProperty("includeTraining");

		int numProductsToPredict = 0;

		if (prop.getProperty("numProductsToPredict").equalsIgnoreCase("ALL")) {
			numProductsToPredict = 99999;
		} else {
			numProductsToPredict = Integer.parseInt(prop.getProperty("numProductsToPredict"));
		}

		System.out.println("***** Properties ****");
		System.out.println("mlUser " + dbUser);
		System.out.println("mlHost " + dbHost);
		System.out.println("mlPort " + dbPort);
		System.out.println("mlPwd " + dbPwd);
		System.out.println("mlOrderCollection " + mlOrderCollection);
		System.out.println("mlDbName " + mlDbName);
		System.out.println("numProductsToPredict " + numProductsToPredict);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("MarkLogicSparkConnector").setMaster("local");
		sparkConf.set("MarkLogic_Host", dbHost);
		sparkConf.set("MarkLogic_Port", prop.getProperty("mlPort"));
		sparkConf.set("MarkLogic_Database", mlDbName);
		sparkConf.set("MarkLogic_User", dbUser);
		sparkConf.set("MarkLogic_Password", dbPwd);

		SparkSession spark = SparkSession.builder().appName("MarkLogicSparkConnector").config(sparkConf).getOrCreate();
		ScalaConnectorOperations mlOperations = new ScalaConnectorOperations();
		Operations operations = new Operations();

		List<Row> training_data = operations
				.getTrainingData(mlOperations.readOrderDataFromConnector(spark.sparkContext(), mlOrderCollection));

		List<String> uniqueProductList = operations.getUniqueProductList(training_data);
		StructType schema = createStructType(new StructField[] { createStructField("year", IntegerType, false),
				createStructField("product", StringType, false), createStructField("quantity", IntegerType, false) });
		System.out.println("Preparing Training Data...");
		Dataset<Row> training_dataset = spark.createDataFrame(training_data, schema);
		training_dataset = training_dataset.groupBy("year", "product").agg(functions.sum("quantity").as("quantity"))
				.orderBy("year");
		StringIndexer indexer = new StringIndexer().setInputCol("product").setOutputCol("product_index");

		Dataset<Row> transformed_training_dataset = indexer.fit(training_dataset).transform(training_dataset);

		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] { "year", "product_index" })
				.setOutputCol("features");
		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		System.out.println("Spawning " + Runtime.getRuntime().availableProcessors() + " threads...");

		int numTasks = 0;
		if (numProductsToPredict == 9999) {
			numTasks = uniqueProductList.size() - 1;
		} else {
			numTasks = numProductsToPredict;
		}
		for (int iCounter = 0; iCounter < numTasks; iCounter++) {
			Dataset<Row> input_for_regression = assembler.transform(transformed_training_dataset)
					.filter(functions.col("product").equalTo(uniqueProductList.get(iCounter)));
			executor.execute(new PredictionsRunnable(iCounter, spark, input_for_regression, indexer, schema,
					uniqueProductList.get(iCounter), modelPath, includeTraining));
		}
		executor.awaitTermination(1, TimeUnit.HOURS);
		executor.shutdown(); // once you are done with ExecutorService
		spark.stop();
	}
}

class PredictionsRunnable implements Runnable {
	int id;
	SparkSession spark;
	Dataset<Row> input_for_regression;
	DatabaseClient client;
	StringIndexer indexer;
	StructType schema;
	String productName;
	String modelPath;
	String includeTraining;

	public PredictionsRunnable(int i, SparkSession spark, Dataset<Row> input_for_regression, StringIndexer indexer,
			StructType schema, String productName, String modelPath, String includeTraining) {
		this.id = i;
		this.spark = spark;
		this.input_for_regression = input_for_regression;
		this.indexer = indexer;
		this.schema = schema;
		this.productName = productName;
		this.client = DatabaseClientFactory.newClient(ScalaConnectorTest.dbHost, ScalaConnectorTest.dbPort,
				ScalaConnectorTest.mlDbName,
				new DatabaseClientFactory.DigestAuthContext(ScalaConnectorTest.dbUser, ScalaConnectorTest.dbPwd));
		this.modelPath = modelPath;
		this.includeTraining = includeTraining;
	}

	public void run() {
		Operations operations = new Operations();
		try {

			System.out.println(
					"Prediction for " + productName + " started by Thread " + Thread.currentThread().getName());
			operations.performPrediction(spark, input_for_regression, client, indexer, schema, productName, modelPath,
					includeTraining);
			System.out
					.println("Prediction for " + productName + " ended by Thread " + Thread.currentThread().getName());
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}
