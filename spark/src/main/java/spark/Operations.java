package spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

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
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.FilteredForestConfiguration;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.NoResponseListener;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteFailureListener;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;

public class Operations {

	public ArrayList<Order> readOrderData(DatabaseClient client, String collection)
			throws JsonParseException, JsonMappingException, IOException {
		JSONDocumentManager mgr = client.newJSONDocumentManager();
		Logger.getRootLogger().setLevel(Level.ERROR);

		String searchOptions = "<search:search xmlns:search=\"http://marklogic.com/appservices/search\">"
				+ "<search:options>" + "<search:additional-query>"
				+ "<cts:collection-query xmlns:cts=\"http://marklogic.com/cts\">" + "<cts:uri>" + collection
				+ "</cts:uri>" + " </cts:collection-query>" + "</search:additional-query>" + "</search:options>"
				+ "</search:search>";
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.enable(SerializationFeature.INDENT_OUTPUT).disable(DeserializationFeature.UNWRAP_ROOT_VALUE);
		ArrayList<Order> lstOrders = new ArrayList<Order>();
		StringHandle queryHandle = new StringHandle(searchOptions).withFormat(Format.XML);
		QueryManager queryMgr = client.newQueryManager();
		queryMgr.setPageLength(999999);
		// create a values definition
		QueryDefinition query = queryMgr.newRawCombinedQueryDefinition(queryHandle);

		SearchHandle resultsHandle = queryMgr.search(query, new SearchHandle());
		MatchDocumentSummary[] results = resultsHandle.getMatchResults();
		for (MatchDocumentSummary result : results) {
			StringHandle content = new StringHandle();
			mgr.read(result.getUri(), content);
			Order order = mapper.readValue(content.get(), Order.class);
			lstOrders.add(order);
		}

		return lstOrders;
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

		List<String> uniqueProductList = new ArrayList();
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

	public void performPrediction(SparkSession spark, Dataset<Row> input_for_regression, DatabaseClient client,
			StringIndexer indexer, StructType schema, String productName) {
		LinearRegression lr = new LinearRegression().setRegParam(0.3).setElasticNetParam(.8).setMaxIter(10).setTol(1E-6)
				.setFeaturesCol("features").setLabelCol("quantity");
		LinearRegressionModel model = lr.fit(input_for_regression);
		
		HyperParameters hyperParameters = new HyperParameters();
		hyperParameters.setElasticNet(model.getElasticNetParam());
		hyperParameters.setTolerance(model.getTol());
		hyperParameters.setNumIterations(model.getMaxIter());
		hyperParameters.setLambda(model.getRegParam());
		
		// input_for_regression.show(200,false);
		// Print the coefficients and intercept for generalized linear
		// regression model

		//// Summarize the model over the training set and print out some
		// metrics
		LinearRegressionTrainingSummary summary = model.summary();
		System.out.println("Root Mean Squared for product  '" + productName + "' is " + summary.rootMeanSquaredError());
		ModelSummary modelSummary = new ModelSummary();
		modelSummary.setRootMeanSquaredError(summary.rootMeanSquaredError());
		// summary.residuals().show(200,false);

		/*
		 * Prepare testdata for next three years
		 */

		VectorAssembler test_data_assembler = new VectorAssembler()
				.setInputCols(new String[] { "year", "product_index" }).setOutputCol("features");
		List<Row> new_test_data = new ArrayList<Row>();
		new_test_data.add(RowFactory.create(1999, productName, 0));
		new_test_data.add(RowFactory.create(2000, productName, 0));
		new_test_data.add(RowFactory.create(2001, productName, 0));
		Dataset<Row> test_data_set = spark.createDataFrame(new_test_data, schema);
		test_data_set = indexer.fit(test_data_set).transform(test_data_set);
		test_data_set = test_data_assembler.transform(test_data_set);
		test_data_set = input_for_regression.union(test_data_set);
		// Prediction
		Dataset<?> predictions = model.transform(test_data_set);
		predictions.show(200, false);

		Encoder<Prediction> predictionEncoder = Encoders.bean(Prediction.class);
		List<Prediction> predictionList = new ArrayList<>();
		predictionList = predictions.as(predictionEncoder).collectAsList();
		
		System.out.println("Inserting Predictions of '" + productName + "' into MarkLogic...");
		try {
			insertPredictions(predictionList, client, productName, hyperParameters, modelSummary);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertPredictions(List<Prediction> predictionList, 
									DatabaseClient client, 
									String productName,
									HyperParameters hyperParameters,
									ModelSummary modelSummary)
			throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.enable(SerializationFeature.INDENT_OUTPUT).disable(DeserializationFeature.UNWRAP_ROOT_VALUE)
				.disable(SerializationFeature.WRAP_ROOT_VALUE);
		JSONDocumentManager mgr = client.newJSONDocumentManager();
		String prodNameForUri = "";
		Date date = new Date();
		PredictionList lstPredictions = new PredictionList();
		lstPredictions.setCreated(date.toString());
		lstPredictions.setProduct(productName);
		lstPredictions.setPredictions(predictionList);
		lstPredictions.setHyperParameters(hyperParameters);
		lstPredictions.setModelSummary(modelSummary);
		StringHandle writeHandle = new StringHandle(mapper.writeValueAsString(lstPredictions));
		DocumentMetadataHandle meta = new DocumentMetadataHandle();
		meta.getCollections().add("predictions");
		prodNameForUri = productName.replaceAll("[^A-Za-z0-9]", "~");
		mgr.write("/predictions/"+prodNameForUri + ".json", meta, writeHandle);
		return;
	}
	
}