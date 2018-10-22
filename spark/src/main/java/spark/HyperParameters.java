package spark;

public class HyperParameters {

	Double lambda;
	Double elasticNet;
	Integer numIterations;
	Double tolerance;

	public Double getLambda() {
		return lambda;
	}

	public void setLambda(Double lambda) {
		this.lambda = lambda;
	}

	public Double getElasticNet() {
		return elasticNet;
	}

	public void setElasticNet(Double elasticNet) {
		this.elasticNet = elasticNet;
	}

	public Integer getNumIterations() {
		return numIterations;
	}

	public void setNumIterations(Integer numIterations) {
		this.numIterations = numIterations;
	}

	public Double getTolerance() {
		return tolerance;
	}

	public void setTolerance(Double tolerance) {
		this.tolerance = tolerance;
	}

}
