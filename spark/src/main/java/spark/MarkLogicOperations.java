package spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;

public class MarkLogicOperations {

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

	public void insertModelMeta(List<String> lstMeta, DatabaseClient client, String productName)
			throws JsonProcessingException {
		JSONDocumentManager mgr = client.newJSONDocumentManager();
		String modelString = lstMeta.get(0);
		StringHandle handle = new StringHandle(modelString);
		DocumentMetadataHandle meta = new DocumentMetadataHandle();
		meta.getCollections().add("models");
		String prodNameForUri = productName.replaceAll("[^A-Za-z0-9]", "~");
		mgr.write("/models/metadata/" + prodNameForUri + ".json", meta, handle);
		return;
	}

	public void insertModels(List<String> lstModels, DatabaseClient client, String productName)
			throws JsonProcessingException {
		JSONDocumentManager mgr = client.newJSONDocumentManager();
		String modelString = lstModels.get(0);
		StringHandle handle = new StringHandle(modelString);
		DocumentMetadataHandle meta = new DocumentMetadataHandle();
		meta.getCollections().add("models");
		String prodNameForUri = productName.replaceAll("[^A-Za-z0-9]", "~");
		mgr.write("/models/data/" + prodNameForUri + ".json", meta, handle);
		return;
	}

	public void insertPredictions(List<Prediction> predictionList, DatabaseClient client, String productName,
			HyperParameters hyperParameters, ModelSummary modelSummary) throws JsonProcessingException {
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
		mgr.write("/predictions/" + prodNameForUri + ".json", meta, writeHandle);
		return;
	}

}
