package spark;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.IOUtils;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.spark.java.connector.lookupOrders;

public class Operations {

	public List<String> tokenizeProducts(String products) {

		List<String> productList = Arrays.asList(products.split("\\s*,\\s*"));
		return productList;
	}

	static ArrayList<String> removeDuplicates(List<String> uniqueProductList) {

		// Store unique items in result.
		ArrayList<String> result = new ArrayList<>();

		// Record encountered Strings in HashSet.
		HashSet<String> set = new HashSet<>();

		// Loop over argument list.
		for (String item : uniqueProductList) {

			// If String is not in set, add it to the list and the set.
			if (!set.contains(item)) {
				result.add(item);
				set.add(item);
			}
		}
		return result;
	}

	public List<String> getUniqueProductList(List<Row> training_data) {

		List<String> uniqueProductList = new ArrayList<String>();
		for (int i = 0; i < training_data.size(); i++) {
			uniqueProductList.add(training_data.get(i).getString(1));
		}
		return removeDuplicates(uniqueProductList);
	}

	public List<Row> getTrainingData(ArrayList<Order> lstOrders) {
		List<Row> data = new ArrayList<Row>();
		int year = 0;
		String[] arrQuantity;
		String[] arrProductName;
		for (int counter = 0; counter < lstOrders.size(); counter++) {
			year = Integer.parseInt(lstOrders.get(counter).getOrderDate().split("-")[0]);
			arrProductName = lstOrders.get(counter).getProductName().replaceAll("[{}]", "").split(",");
			arrQuantity = lstOrders.get(counter).getQuantity().replaceAll("[{}]", "").split(",");
			for (int i = 0; i < arrProductName.length; i++) {
				data.add(RowFactory.create(year, arrProductName[i], Integer.parseInt(arrQuantity[i])));
			}
		}
		return data;
	}

	public List<Row> getTrainingDataFromOrderSet(List<String> lstOrders) {

		List<Row> data = new ArrayList<Row>();
		int year = 0;
		String[] arrQuantity;
		String[] arrProductName;
		String rePattern = ",(?=[^{}]*\\})";
		for (int counter = 1; counter < lstOrders.size(); counter++) {
			String[] temp = lstOrders.get(counter).replace("\"", "").replaceAll(rePattern, "#").split(",");
			year = Integer.parseInt(temp[0].split("-")[0]);
			arrProductName = temp[2].replaceAll("[{}]", "").split("#");
			arrQuantity = temp[1].replaceAll("[{}]", "").split("#");
			for (int i = 0; i < arrProductName.length; i++) {
				data.add(RowFactory.create(year, arrProductName[i].trim(), Integer.parseInt(arrQuantity[i])));
			}
		}
		return data;

	}

	public void performEndpointPredictions(SparkSession spark, DatabaseClient noRestClient, DatabaseClient restClient,
			String productName, String moduleName, String modulePath, String modelPath, String includeTraining)
			throws IOException {
		lookupOrders ec = lookupOrders.on(noRestClient);
		Reader rdr = ec.lookUpOrders(productName);
		String lines[] = IOUtils.toString(rdr).split("\\n");
		List<String> rows = new ArrayList<String>();
		for (String line : lines) {
			rows.add(line);
		}
		Dataset<String> orderSet = spark.createDataset(rows, Encoders.STRING());

		/*
		 * Use the orderSet Dataset for the predictions.
		 */
		List<String> orders = orderSet.collectAsList();

		List<Row> training_data = getTrainingDataFromOrderSet(orders);

		StructType schema = createStructType(new StructField[] { createStructField("year", IntegerType, false),
				createStructField("product", StringType, false), createStructField("quantity", IntegerType, false) });
		Date start = new Date();
		System.out.println("Preparing training data for '" + productName + "'");
		Dataset<Row> training_dataset = spark.createDataFrame(training_data, schema);
		training_dataset = training_dataset.groupBy("year", "product").agg(functions.sum("quantity").as("quantity"))
				.orderBy("year");
		StringIndexer indexer = new StringIndexer().setInputCol("product").setOutputCol("product_index");
		Dataset<Row> transformed_training_dataset = indexer.fit(training_dataset).transform(training_dataset);
		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] { "year", "product_index" })
				.setOutputCol("features");
		Dataset<Row> input_for_regression = assembler.transform(transformed_training_dataset)
				.filter(functions.col("product").equalTo(productName));
		Date end = new Date();
		System.out.println("Preparing training data ended for '" + productName + "' in "
				+ (end.getTime() - start.getTime()) / 1000 + " seconds");
		performPrediction(spark, input_for_regression, restClient, indexer, schema, productName, modelPath,
				includeTraining);
	}

	public void performOpticPrediction(SparkSession spark, DatabaseClient client, String productName, String modelPath,
			String includeTraining) throws IOException {

		JavaConnectorOperations mlOperations = new JavaConnectorOperations();
		/*
		 * Handle the possible single quote before calling MarkLogic Queries
		 */
		String sqlCondition = "ProductName like '%" + productName.replace("'", "''") + "%'";
		Dataset<String> orderSet = mlOperations.getOrdersWithProductName(spark, "orders", "products", sqlCondition);
//		System.out.println("Output from Optic connector...");
//		orderSet.show();
		/*
		 * Use the orderSet Dataset for the predictions.
		 */
		List<String> orders = orderSet.collectAsList();

		List<Row> training_data = getTrainingDataFromOrderSet(orders);

		StructType schema = createStructType(new StructField[] { createStructField("year", IntegerType, false),
				createStructField("product", StringType, false), createStructField("quantity", IntegerType, false) });
		System.out.println("Preparing Training Data for '" + productName + "'");
		Dataset<Row> training_dataset = spark.createDataFrame(training_data, schema);
		training_dataset = training_dataset.groupBy("year", "product").agg(functions.sum("quantity").as("quantity"))
				.orderBy("year");

		StringIndexer indexer = new StringIndexer().setInputCol("product").setOutputCol("product_index");

		Dataset<Row> transformed_training_dataset = indexer.fit(training_dataset).transform(training_dataset);

		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] { "year", "product_index" })
				.setOutputCol("features");
		Dataset<Row> input_for_regression = assembler.transform(transformed_training_dataset)
				.filter(functions.col("product").equalTo(productName));
		// input_for_regression.show();
		performPrediction(spark, input_for_regression, client, indexer, schema, productName, modelPath,
				includeTraining);
		// if insert of predictions required, use the other signature with
		// Database client.
	}

	public void performPrediction(SparkSession spark, Dataset<Row> input_for_regression, StringIndexer indexer,
			StructType schema, String productName, String modelPath, String includeTraining) throws IOException {
		performPrediction(spark, input_for_regression, null, indexer, schema, productName, modelPath, includeTraining);
	}

	public void performPrediction(SparkSession spark, Dataset<Row> input_for_regression, DatabaseClient client,
			StringIndexer indexer, StructType schema, String productName, String modelPath, String includeTraining)
			throws IOException {
		Date start = new Date();
		MarkLogicOperations mlOperations = new MarkLogicOperations();
		ModelSummary modelSummary = null;
		HyperParameters hyperParameters = null;
		LinearRegressionTrainingSummary summary = null;
		LinearRegressionModel model = null;
		Dataset<?> predictions = null;
		String path = modelPath + productName.replaceAll("[^A-Za-z0-9]", "~");
		Dataset<String> modelJSON = null;
		Dataset<String> metaJSON = null;
		List<String> lstModel = new ArrayList<>();
		List<String> lstMeta = new ArrayList<>();
		if (includeTraining.equalsIgnoreCase("TRUE")) {
			//System.out.println("Training started for '" + productName + "'");
			LinearRegression lr = new LinearRegression().setRegParam(0.3).setElasticNetParam(.8).setMaxIter(10)
					.setTol(1E-6).setFeaturesCol("features").setLabelCol("quantity");

			model = lr.fit(input_for_regression);
			Date end = new Date();
//			System.out.println("Training ended for '" + productName + "' in " + (end.getTime() - start.getTime()) / 1000
//					+ " seconds");

			model.write().overwrite().save(path); /*
							 * Ideally this persistence should be avoided and
							 * persist the model directly as JSON into MarkLogic
							 */
			modelJSON = spark.read().parquet(path + "/data/").toJSON();

			Encoder<String> modelEncoder = Encoders.STRING();
			lstModel = modelJSON.as(modelEncoder).collectAsList();
			mlOperations.insertModels(lstModel, client, productName);

			metaJSON = spark.read().json(path + "/metadata/").toJSON();
			Encoder<String> metaEncoder = Encoders.STRING();
			lstMeta = metaJSON.as(metaEncoder).collectAsList();
			mlOperations.insertModelMeta(lstMeta, client, productName);

			hyperParameters = new HyperParameters();
			hyperParameters.setElasticNet(model.getElasticNetParam());
			hyperParameters.setTolerance(model.getTol());
			hyperParameters.setNumIterations(model.getMaxIter());
			hyperParameters.setLambda(model.getRegParam());

			summary = model.summary();
			modelSummary = new ModelSummary();
			modelSummary.setRootMeanSquaredError(summary.rootMeanSquaredError());

			/* Prepare testdata for next three years and predict */
		}

//		System.out.println("Prediction started for '" + productName + "'");
		VectorAssembler test_data_assembler = new VectorAssembler()
				.setInputCols(new String[] { "year", "product_index" }).setOutputCol("features");
		List<Row> new_test_data = new ArrayList<Row>();
		new_test_data.add(RowFactory.create(2018, productName, 0));
		new_test_data.add(RowFactory.create(2019, productName, 0));
		new_test_data.add(RowFactory.create(2020, productName, 0));
		Dataset<Row> test_data_set = spark.createDataFrame(new_test_data, schema);
		test_data_set = indexer.fit(test_data_set).transform(test_data_set);
		test_data_set = test_data_assembler.transform(test_data_set);
		test_data_set = input_for_regression.union(test_data_set);
		// Prediction

		if (!includeTraining.equalsIgnoreCase("TRUE")) {
			// Read the model back
			LinearRegressionModel sameModel = LinearRegressionModel.read()
					.load(modelPath + productName.replaceAll("[^A-Za-z0-9]", "~"));

			predictions = sameModel.transform(test_data_set);
		} else {
			predictions = model.transform(test_data_set);

		}
		//predictions.show(200, false);
		Date end = new Date();
//		System.out.println("Prediction ended for '" + productName + "' in " + (end.getTime() - start.getTime()) / 1000
//				+ " seconds.");
		Encoder<Prediction> predictionEncoder = Encoders.bean(Prediction.class);
		List<Prediction> predictionList = new ArrayList<>();
		predictionList = predictions.as(predictionEncoder).collectAsList();
		if (client != null) {
//			System.out.println("Inserting Predictions of '" + productName + "' into MarkLogic...");
			try {
				mlOperations.insertPredictions(predictionList, client, productName, hyperParameters, modelSummary);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}
	}

}