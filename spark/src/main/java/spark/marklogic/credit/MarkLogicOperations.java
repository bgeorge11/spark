package spark.marklogic.credit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.expression.PlanBuilder.ModifyPlan;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RowManager;

public class MarkLogicOperations {

	public List<String> readLoanData(DatabaseClient client, String collection)
			throws JsonParseException, JsonMappingException, IOException {
		RowManager rowMgr = client.newRowManager();
		PlanBuilder p = rowMgr.newPlanBuilder();
		String sqlCondition = "Loan_ID like '%LP%'";

		ModifyPlan plan = p.fromView("Loan", "LoanDetails")
							.select()
							.where(p.sqlCondition(sqlCondition));
		StringHandle stringHandle = new StringHandle();
		stringHandle.setMimetype("text/csv");
		rowMgr.resultDoc(plan, stringHandle);
		String lines[] = stringHandle.get().split("\\n");
		List<String> rows = new ArrayList<String>();
		for(String line: lines) {
		rows.add(line);
		}
		return rows;
		
	}
	
	public List<String> readTestLoanData(DatabaseClient client, String collection)
			throws JsonParseException, JsonMappingException, IOException {
		RowManager rowMgr = client.newRowManager();
		PlanBuilder p = rowMgr.newPlanBuilder();
		String sqlCondition = "Loan_ID like '%LP%'";

		ModifyPlan plan = p.fromView("Loan", "TrainLoanDetails")
							.select()
							.where(p.sqlCondition(sqlCondition));
		StringHandle stringHandle = new StringHandle();
		stringHandle.setMimetype("text/csv");
		rowMgr.resultDoc(plan, stringHandle);
		String lines[] = stringHandle.get().split("\\n");
		List<String> rows = new ArrayList<String>();
		for(String line: lines) {
		rows.add(line);
		}
		return rows;
		
	}
	
	public List<Row> getLoanData(List<String> lstLoans) {
		List<Row> data = new ArrayList<Row>();
		String[] parts;
		
		for (int counter = 1; counter < lstLoans.size(); counter++) {
			parts = lstLoans.get(counter).split(",");
			Double applIncome = 0.0;
			Double coIncome = 0.0;
			Double loanAmt = 0.0;
			Double loanTerm = 0.0;
			Double creditHist = 0.0; 
			if (!parts[6].trim().isEmpty())
				applIncome = Double.parseDouble(parts[6]);
			if (!parts[7].trim().isEmpty())
				coIncome = Double.parseDouble(parts[7]);
			if (!parts[8].trim().isEmpty())
				loanAmt = Double.parseDouble(parts[8]);
			if (!parts[9].trim().isEmpty())
				loanTerm = Double.parseDouble(parts[9]);
			if (!parts[10].trim().isEmpty())
				creditHist = Double.parseDouble(parts[10]);
			
			data.add(RowFactory.create(parts[0],
									   parts[1],
									   parts[2],
									   parts[3],
									   parts[4],
									   parts[5],
									   applIncome,
									   coIncome,
									   loanAmt,
									   loanTerm,
									   creditHist,
									   parts[11],
									   parts[12]
										));
		}
		return data;
	}
	
	
}
