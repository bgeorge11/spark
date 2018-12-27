package spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.spark.java.connector.MarkLogicDataSetPartitioner;
import com.marklogic.spark.java.connector.MarkLogicPartition;
import com.marklogic.spark.java.connector.SparkDocument;


public class JavaConnectorOperations {
	
	public Dataset<String> getOrdersWithProductName (SparkSession spark,
													  String schema,
													  String view,
													  String sqlCondition
													  )
	{
		MarkLogicDataSetPartitioner mlParts = new MarkLogicDataSetPartitioner();
		return mlParts.getDataUsingTemplate(spark,schema,view,sqlCondition);
	}
	
	
	public ArrayList<Order> readOrderDataFromConnector (SparkContext sc, 
														String collection
														) 
										throws JsonParseException, JsonMappingException, IOException 
	{
		StructuredQueryBuilder queryBuilder = new StructuredQueryBuilder();
		ObjectMapper mapper = new ObjectMapper()
								.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
								.enable(SerializationFeature.INDENT_OUTPUT)
								.disable(DeserializationFeature.UNWRAP_ROOT_VALUE);
		
		String searchOptions = "<search:search xmlns:search=\"http://marklogic.com/appservices/search\">"
				+ "<search:options>" 
				+ "<search:additional-query>"
				+ "<cts:collection-query xmlns:cts=\"http://marklogic.com/cts\">" 
				+ "<cts:uri>" + collection
				+ "</cts:uri>" 
				+ " </cts:collection-query>" 
				+ "</search:additional-query>" 
				+ "</search:options>"
				+ "</search:search>";
		StructuredQueryDefinition query = queryBuilder.collection(collection);
		query.setOptionsName(searchOptions);
		MarkLogicDataSetPartitioner mlParts = new MarkLogicDataSetPartitioner();
		MarkLogicPartition parts[] = mlParts.getPartitions(sc, query.serialize());
		System.out.println("Number of Document parts = " + parts.length);
		ArrayList<Order> lstOrders = new ArrayList<Order>();

		for (int i=0; i<parts.length; i++) {
			List<SparkDocument> sparkDocs =  mlParts.getDocumentsFromParts(sc,parts[i]);
			Iterator<SparkDocument> sparkDocsIterator = sparkDocs.iterator();
			while (sparkDocsIterator.hasNext()) {
				SparkDocument doc = (SparkDocument) sparkDocsIterator.next();
				String docContent = doc.getContentDocument().toString();
				Order order = mapper.readValue(docContent, Order.class);
				lstOrders.add(order);
			}
		}
		return lstOrders;
} 
	}
