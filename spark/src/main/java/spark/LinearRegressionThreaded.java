/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

public class LinearRegressionThreaded {

	public static void main(String[] args)
			throws JsonParseException, JsonMappingException, IOException, InterruptedException {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getRootLogger().setLevel(Level.ERROR);

		String fileName = args[0];
		Properties prop = new Properties();
		FileInputStream input = null;
		input = new FileInputStream(fileName);
		System.out.println("Loading properties... " + fileName);
		prop.load(input);

		String dbUser = prop.getProperty("mlUser");
		String dbHost = prop.getProperty("mlHost");
		int dbPort = Integer.parseInt(prop.getProperty("mlPort"));
		String dbPwd = prop.getProperty("mlPwd");
		String mlOrderCollection = prop.getProperty("mlOrderCollection");
		String mlDbName = prop.getProperty("mlDbName");

		String modelPath = prop.getProperty("pathToStoreModel");
		String includeTraining = prop.getProperty("includeTraining");

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
		DatabaseClient client = DatabaseClientFactory.newClient(dbHost, dbPort, mlDbName,
				new DatabaseClientFactory.DigestAuthContext(dbUser, dbPwd));
		System.out.println("Creating Spark Session...");
		SparkSession spark = SparkSession.builder().appName("MarkLogicSparkIntegration").getOrCreate();

		Operations operations = new Operations();
		MarkLogicOperations mlOperations = new MarkLogicOperations();
		System.out.println("Connecting to MarkLogic for fetching order data...");
		ArrayList<Order> lstOrders = mlOperations.readOrderData(client, mlOrderCollection);
		System.out.println("Number of Orders fetched ==> " + lstOrders.size());
		List<Row> training_data = operations.getTrainingData(lstOrders);
		List<String> uniqueProductList = operations.getUniqueProductList(training_data);
		System.out.println("Number of unique products in orders ==> " + uniqueProductList.size());

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
		if (numProductsToPredict == 99999) {
			numTasks = uniqueProductList.size();
		} else {
			numTasks = numProductsToPredict;
		}

		for (int iCounter = 0; iCounter < numTasks; iCounter++) {
			Dataset<Row> input_for_regression = assembler.transform(transformed_training_dataset)
					.filter(functions.col("product").equalTo(uniqueProductList.get(iCounter)));
			executor.execute(new MyRunnable(iCounter, spark, input_for_regression, client, indexer, schema,
					uniqueProductList.get(iCounter), modelPath, includeTraining));
		}
		executor.awaitTermination(20, TimeUnit.MINUTES);
		executor.shutdown(); // once you are done with ExecutorService
		spark.stop();
		// client.release();
	}
}

class MyRunnable implements Runnable {
	int id;
	SparkSession spark;
	Dataset<Row> input_for_regression;
	DatabaseClient client;
	StringIndexer indexer;
	StructType schema;
	String productName;
	String modelPath;
	String includeTraining;

	public MyRunnable(int i, SparkSession spark, Dataset<Row> input_for_regression, DatabaseClient client,
			StringIndexer indexer, StructType schema, String productName, String modelPath, String includeTraining) {
		this.id = i;
		this.spark = spark;
		this.input_for_regression = input_for_regression;
		this.client = client;
		this.indexer = indexer;
		this.schema = schema;
		this.productName = productName;
		this.modelPath = modelPath;
		this.includeTraining = includeTraining;
	}

	public void run() {
		Operations operations = new Operations();
		Date start = new Date();
		try {
			System.out.println(
					"Prediction for " + productName + " started by Thread " + Thread.currentThread().getName());
			operations.performPrediction(spark, input_for_regression, client, indexer, schema, productName, modelPath,
					includeTraining);
			Date end = new Date();
			System.out
			.println("Prediction for " + productName + " ended by Thread " + Thread.currentThread().getName() + " in  " + (end.getTime() - start.getTime()) / 1000 + " seconds.");
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}
