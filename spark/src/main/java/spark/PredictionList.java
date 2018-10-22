package spark;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonRootName;

@JsonPropertyOrder({ "product", "created", "predictions", "hyperParameters", "modelSummary"})
public class PredictionList {

	@JsonProperty("predictions")
	private List<Prediction> predictions = null;
	@JsonProperty("product")
	private String product;
	@JsonProperty("created")
	private String created;
	@JsonProperty("hyperParameters")
	private HyperParameters hyperParameters;
	@JsonProperty("modelSummary")
	private ModelSummary modelSummary;

	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public List<Prediction> getPredictions() {
		return predictions;
	}

	public void setPredictions(List<Prediction> predictions) {
		this.predictions = predictions;
	}

	public HyperParameters getHyperParameters() {
		return hyperParameters;
	}

	public void setHyperParameters(HyperParameters hyperParameters) {
		this.hyperParameters = hyperParameters;
	}

	public ModelSummary getModelSummary() {
		return modelSummary;
	}

	public void setModelSummary(ModelSummary modelSummary) {
		this.modelSummary = modelSummary;
	}

}
