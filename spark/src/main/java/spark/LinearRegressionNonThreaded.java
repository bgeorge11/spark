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
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

public class LinearRegressionNonThreaded {

	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
		
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

		int numProductsToPredict = 0;
		
		if (prop.getProperty("numProductsToPredict").equalsIgnoreCase("ALL")) {
			numProductsToPredict = 99999; 
		}
		else { 
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

		DatabaseClient client = DatabaseClientFactory.newClient("localhost", 8000, "bnsf-content",
				new DatabaseClientFactory.DigestAuthContext("admin", "admin"));
		Operations operations = new Operations();
		ArrayList<Order> lstOrders = operations.readOrderData(client, mlOrderCollection);
		List<Row> training_data = operations.getTrainingData(lstOrders);
		List<String> uniqueProductList = operations.getUniqueProductList(training_data);
		System.out.println("Number of unique Products::" + uniqueProductList.size());

		SparkSession spark = SparkSession.builder().appName("MarkLogicSparkIntegrationNonThreaded").getOrCreate();


		StructType schema = createStructType(new StructField[] { createStructField("year", IntegerType, false),
				createStructField("product", StringType, false), createStructField("quantity", IntegerType, false) });

		Dataset<Row> training_dataset = spark.createDataFrame(training_data, schema);

		training_dataset = training_dataset.groupBy("year", "product").agg(functions.sum("quantity").as("quantity"))
				.orderBy("year");
		StringIndexer indexer = new StringIndexer().setInputCol("product").setOutputCol("product_index");

		Dataset<Row> transformed_training_dataset = indexer.fit(training_dataset).transform(training_dataset);

		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] { "year", "product_index" })
				.setOutputCol("features");

		int numTasks = 0;
		if (numProductsToPredict == 9999) {
			numTasks = uniqueProductList.size();
		}
		else
		{
			numTasks = numProductsToPredict;
		}
		
		for (int iCounter = 0; iCounter < numTasks; iCounter++) {
			System.out.println("Doing prediction for ::" + uniqueProductList.get(iCounter));

			Dataset<Row> input_for_regression = assembler.transform(transformed_training_dataset)
					.filter(functions.col("product").equalTo(uniqueProductList.get(iCounter)));

			LinearRegression lr = new LinearRegression().setRegParam(0.3).setElasticNetParam(.8).setMaxIter(10)
					.setTol(1E-6).setFeaturesCol("features").setLabelCol("quantity");
			LinearRegressionModel model = lr.fit(input_for_regression);
			
			HyperParameters hyperParameters = new HyperParameters();
			hyperParameters.setElasticNet(model.getElasticNetParam());
			hyperParameters.setTolerance(model.getTol());
			hyperParameters.setNumIterations(model.getMaxIter());
			hyperParameters.setLambda(model.getRegParam());
			
			// input_for_regression.show(200,false);
			// Print the coefficients and intercept for generalized linear
			// regression model

			// // Summarize the model over the training set and print out some
			// metrics
			LinearRegressionTrainingSummary summary = model.summary();
			System.out.println("Root Mean Squared = " + summary.rootMeanSquaredError());
			ModelSummary modelSummary = new ModelSummary();
			modelSummary.setRootMeanSquaredError(summary.rootMeanSquaredError());
			// summary.residuals().show(200,false);

			/*
			 * Prepare testdata for next three years
			 */

			VectorAssembler test_data_assembler = new VectorAssembler()
					.setInputCols(new String[] { "year", "product_index" }).setOutputCol("features");
			List<Row> new_test_data = new ArrayList<Row>();
			new_test_data.add(RowFactory.create(1999, uniqueProductList.get(iCounter), 0));
			new_test_data.add(RowFactory.create(2000, uniqueProductList.get(iCounter), 0));
			new_test_data.add(RowFactory.create(2001, uniqueProductList.get(iCounter), 0));
			Dataset<Row> test_data_set = spark.createDataFrame(new_test_data, schema);
			test_data_set = indexer.fit(test_data_set).transform(test_data_set);

			test_data_set = test_data_assembler.transform(test_data_set);
			test_data_set = input_for_regression.union(test_data_set);
			// Prediction
			Dataset<?> predictions = model.transform(test_data_set);
			predictions.show(200,false);
			
			Encoder<Prediction> predictionEncoder = Encoders.bean(Prediction.class);
			List<Prediction> predictionList = new ArrayList<>();
			predictionList = predictions.as(predictionEncoder).collectAsList();
			operations.insertPredictions(predictionList, 
										 client, 
										 uniqueProductList.get(iCounter),
										 hyperParameters,
										 modelSummary);
		}
		spark.stop();
		// client.release();

	}
}