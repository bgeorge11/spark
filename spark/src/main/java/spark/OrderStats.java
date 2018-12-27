package spark;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderStats {
	
	@JsonProperty("totalOrderCount")
	private long totalOrderCount;
	@JsonProperty("orderCountInBatch")
	private long orderCountInBatch;
	public long getTotalOrderCount() {
		return totalOrderCount;
	}
	public void setTotalOrderCount(long totalOrderCount) {
		this.totalOrderCount = totalOrderCount;
	}
	public long getOrderCountInBatch() {
		return orderCountInBatch;
	}
	public void setOrderCountInBatch(long orderCountInBatch) {
		this.orderCountInBatch = orderCountInBatch;
	}
	@JsonProperty("statsTime")
	private String statsTime;
	
	public String getStatsTime() {
		return statsTime;
	}
	public void setStatsTime(String string) {
		this.statsTime = string;
	}

}
