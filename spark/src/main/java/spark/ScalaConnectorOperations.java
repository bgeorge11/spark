package spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
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
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.spark.marklogic.SparkDocument;
import com.marklogic.spark.rdd.MarkLogicDocumentRDD;

import scala.collection.Iterator;

public class ScalaConnectorOperations {
	
	
	public ArrayList<Order> readOrderDataFromConnector (SparkContext sc, String collection) throws JsonParseException, JsonMappingException, IOException 
	{
		StructuredQueryBuilder queryBuilder = new StructuredQueryBuilder();
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.enable(SerializationFeature.INDENT_OUTPUT).disable(DeserializationFeature.UNWRAP_ROOT_VALUE);
		
		String searchOptions = "<search:search xmlns:search=\"http://marklogic.com/appservices/search\">"
				+ "<search:options>" + "<search:additional-query>"
				+ "<cts:collection-query xmlns:cts=\"http://marklogic.com/cts\">" + "<cts:uri>" + collection
				+ "</cts:uri>" + " </cts:collection-query>" + "</search:additional-query>" + "</search:options>"
				+ "</search:search>";
		StructuredQueryDefinition query = queryBuilder.collection(collection);
		query.setOptionsName(searchOptions);
		MarkLogicDocumentRDD mlRdd = new MarkLogicDocumentRDD(sc, query.serialize());
		Partition parts[] = mlRdd.accessParts();
		ArrayList<Order> lstOrders = new ArrayList<Order>();
		System.out.println("Number of Document parts = " + parts.length);
		for (int i=0; i<parts.length; i++) {
			Iterator<SparkDocument> sparkDocumentIterator =  mlRdd.compute(parts[i], null);
			while (sparkDocumentIterator.hasNext()) {
				SparkDocument doc = (SparkDocument) sparkDocumentIterator.next();
				String docContent = doc.getContentDocument().toString();
				Order order = mapper.readValue(docContent, Order.class);
				lstOrders.add(order);
			}
		}
		System.out.println("Number of Orders Fetched = " + lstOrders.size());
		return lstOrders;
	}
		
}