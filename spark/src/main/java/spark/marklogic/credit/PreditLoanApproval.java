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

package spark.marklogic.credit;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.io.FileInputStream;
import java.io.IOException;
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
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

public class PreditLoanApproval {

	public static void main(String[] args)
			throws JsonParseException, JsonMappingException, IOException, InterruptedException {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getRootLogger().setLevel(Level.ERROR);

		//String fileName = args[0];
		String fileName = "C:\\spark\\config.properties";
		Properties prop = new Properties();
		FileInputStream input = null;
		input = new FileInputStream(fileName);
		System.out.println("Loading properties... " + fileName);
		prop.load(input);

		String dbUser = prop.getProperty("mlUser");
		String dbHost = prop.getProperty("mlHost");
		int dbPort = Integer.parseInt(prop.getProperty("mlPort"));
		String dbPwd = prop.getProperty("mlPwd");
		String mlLoanCollection = prop.getProperty("mlLoanCollection");
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
		System.out.println("mlLoanCollection " + mlLoanCollection);
		System.out.println("mlDbName " + mlDbName);
		System.out.println("numProductsToPredict " + numProductsToPredict);
		DatabaseClient client = DatabaseClientFactory.newClient(dbHost, dbPort, mlDbName,
				new DatabaseClientFactory.DigestAuthContext(dbUser, dbPwd));
		System.out.println("Creating Spark Session...");
		SparkSession spark = SparkSession.builder().appName("MarkLogicSparkIntegration").getOrCreate();

		MarkLogicOperations mlOperations = new MarkLogicOperations();
		System.out.println("Connecting to MarkLogic for fetching loan data...");
		List<String> lstLoans = mlOperations.readLoanData(client, "loan");
		Dataset<Row> df = spark.createDataset(lstLoans,Encoders.STRING()).toDF();
		List<Row> lstRowLoans = mlOperations.getLoanData(lstLoans);
		
		StructType schema = createStructType(new StructField[] { createStructField("Loan_ID", StringType, false),
																createStructField("Gender", StringType, true), 
																createStructField("Married", StringType, true),
																createStructField("Dependents", StringType, true),
																createStructField("Education", StringType, true),
																createStructField("Self_Employed", StringType, true),
																createStructField("ApplicantIncome", DoubleType, true),
																createStructField("CoapplicantIncome", DoubleType, true),
																createStructField("LoanAmount", DoubleType, true),
																createStructField("Loan_Amount_Term", DoubleType, true),
																createStructField("Credit_History", DoubleType, true),
																createStructField("Property_Area", StringType, true),
																createStructField("Loan_Status", StringType, true)
																});
		Dataset<Row> training_dataset = spark.createDataFrame(lstRowLoans, schema);
												  ;
		training_dataset.printSchema();
		StringIndexer genderIndexer = new StringIndexer().setInputCol("Gender").setOutputCol("Gender_index").setHandleInvalid("keep");
		StringIndexer marriageIndexer = new StringIndexer().setInputCol("Married").setOutputCol("Married_index");
		StringIndexer educationIndexer = new StringIndexer().setInputCol("Education").setOutputCol("Education_index");
		StringIndexer employmentIndexer = new StringIndexer().setInputCol("Self_Employed").setOutputCol("Self_Employed_index");
		StringIndexer propertyAreaIndexer = new StringIndexer().setInputCol("Property_Area").setOutputCol("Property_Area_index");
		StringIndexer loanStatusIndexer = new StringIndexer().setInputCol("Loan_Status").setOutputCol("Loan_Status_index");
		StringIndexer dependentIndexer = new StringIndexer().setInputCol("Dependents").setOutputCol("Dependents_index");
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{genderIndexer,
																		marriageIndexer,
																		educationIndexer,
																		employmentIndexer,
																		propertyAreaIndexer,
																		loanStatusIndexer,
																		dependentIndexer
																		});
		Dataset<Row> transformed_training_dataset = pipeline.fit(training_dataset).transform(training_dataset);
		transformed_training_dataset.printSchema();
		transformed_training_dataset.show(10, 0, true);
		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] { "Gender_index", 
																					"Married_index",
																					"Education_index",
																					"Self_Employed_index",
																					"Property_Area_index",
																					"Loan_Status_index",
																					"Dependents_index",
																					"ApplicantIncome",
																					"CoapplicantIncome",
																					"LoanAmount",
																					"Loan_Amount_Term",
																					"Credit_History"
																					})
									.setOutputCol("features");
		
		Dataset<Row> regression_input = assembler.transform(transformed_training_dataset);
		regression_input.show(10, 0, true);
		LinearRegression lr = new LinearRegression().setRegParam(0.3).setElasticNetParam(.8).setMaxIter(10)
				.setTol(1E-6).setFeaturesCol("features").setLabelCol("Loan_Status_index");

		LinearRegressionModel model = lr.fit(regression_input);
		LinearRegressionTrainingSummary summary = model.summary();
		System.out.println("RMSE" + summary.rootMeanSquaredError());
		model.write().overwrite().save(modelPath); 
		Dataset<String> modelJSON = spark.read().parquet(modelPath + "/data/").toJSON();
		modelJSON.show(false);
		
		List<String> lstTestLoans = mlOperations.readTestLoanData(client, "loan");
		Dataset<Row> testDF = spark.createDataset(lstTestLoans,Encoders.STRING()).toDF();
		List<Row> lstTestRowLoans = mlOperations.getLoanData(lstLoans);
		Dataset<Row> test_dataset = spark.createDataFrame(lstTestRowLoans, schema);
		Dataset<Row> transformed_test_dataset = pipeline.fit(test_dataset).transform(test_dataset);
		VectorAssembler test_assembler = new VectorAssembler().setInputCols(new String[] { "Gender_index", 
				"Married_index",
				"Education_index",
				"Self_Employed_index",
				"Property_Area_index",
				"Loan_Status_index",
				"Dependents_index",
				"ApplicantIncome",
				"CoapplicantIncome",
				"LoanAmount",
				"Loan_Amount_Term",
				"Credit_History"
				})
				.setOutputCol("features");
		
		Dataset<?> predictions = model.transform(test_assembler.transform(transformed_test_dataset));
		
		predictions.show(400, 0, true);
		

		
		
		spark.stop();
		// client.release();
	}
}
