package spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonRootName;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "year", "quantity", "prediction" })
public class Prediction implements Serializable
{

@JsonProperty("year")
private Integer year;
@JsonProperty("quantity")
private Long quantity;
@JsonProperty("prediction")
private Double prediction;
@JsonIgnore
private Map<String, Object> additionalProperties = new HashMap<String, Object>();
private final static long serialVersionUID = 540951386806745114L;

@JsonProperty("year")
public Integer getYear() {
return year;
}

@JsonProperty("year")
public void setYear(Integer year) {
this.year = year;
}

@JsonProperty("quantity")
public Long getQuantity() {
return quantity;
}

@JsonProperty("quantity")
public void setQuantity(Long quantity) {
this.quantity = quantity;
}

@JsonProperty("prediction")
public Double getPrediction() {
return prediction;
}

@JsonProperty("prediction")
public void setPrediction(Double prediction) {
this.prediction = prediction;
}

@JsonAnyGetter
public Map<String, Object> getAdditionalProperties() {
return this.additionalProperties;
}

@JsonAnySetter
public void setAdditionalProperty(String name, Object value) {
this.additionalProperties.put(name, value);
}

}