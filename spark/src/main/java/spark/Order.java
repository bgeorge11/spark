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

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "order_id", "order_date", "company_name", "contact_name", "contact_title", "address", "region",
		"postal_code", "country", "phone", "order_details_count", "unit_price", "quantity", "discount", "product_name",
		"supplied_id", "category_id" })
public class Order implements Serializable {

	@JsonProperty("order_id")
	private String orderId;
	@JsonProperty("order_date")
	private String orderDate;
	@JsonProperty("company_name")
	private String companyName;
	@JsonProperty("contact_name")
	private String contactName;
	@JsonProperty("contact_title")
	private String contactTitle;
	@JsonProperty("address")
	private String address;
	@JsonProperty("region")
	private Object region;
	@JsonProperty("postal_code")
	private String postalCode;
	@JsonProperty("country")
	private String country;
	@JsonProperty("phone")
	private String phone;
	@JsonProperty("order_details_count")
	private String orderDetailsCount;
	@JsonProperty("unit_price")
	private String unitPrice;
	@JsonProperty("quantity")
	private String quantity;
	@JsonProperty("discount")
	private String discount;
	@JsonProperty("product_name")
	private String productName;
	@JsonProperty("supplied_id")
	private Object suppliedId;
	@JsonProperty("category_id")
	private String categoryId;
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();
	private final static long serialVersionUID = -4095133254151673100L;

	@JsonProperty("order_id")
	public String getOrderId() {
		return orderId;
	}

	@JsonProperty("order_id")
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	@JsonProperty("order_date")
	public String getOrderDate() {
		return orderDate;
	}

	@JsonProperty("order_date")
	public void setOrderDate(String orderDate) {
		this.orderDate = orderDate;
	}

	@JsonProperty("company_name")
	public String getCompanyName() {
		return companyName;
	}

	@JsonProperty("company_name")
	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	@JsonProperty("contact_name")
	public String getContactName() {
		return contactName;
	}

	@JsonProperty("contact_name")
	public void setContactName(String contactName) {
		this.contactName = contactName;
	}

	@JsonProperty("contact_title")
	public String getContactTitle() {
		return contactTitle;
	}

	@JsonProperty("contact_title")
	public void setContactTitle(String contactTitle) {
		this.contactTitle = contactTitle;
	}

	@JsonProperty("address")
	public String getAddress() {
		return address;
	}

	@JsonProperty("address")
	public void setAddress(String address) {
		this.address = address;
	}

	@JsonProperty("region")
	public Object getRegion() {
		return region;
	}

	@JsonProperty("region")
	public void setRegion(Object region) {
		this.region = region;
	}

	@JsonProperty("postal_code")
	public String getPostalCode() {
		return postalCode;
	}

	@JsonProperty("postal_code")
	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}

	@JsonProperty("country")
	public String getCountry() {
		return country;
	}

	@JsonProperty("country")
	public void setCountry(String country) {
		this.country = country;
	}

	@JsonProperty("phone")
	public String getPhone() {
		return phone;
	}

	@JsonProperty("phone")
	public void setPhone(String phone) {
		this.phone = phone;
	}

	@JsonProperty("order_details_count")
	public String getOrderDetailsCount() {
		return orderDetailsCount;
	}

	@JsonProperty("order_details_count")
	public void setOrderDetailsCount(String orderDetailsCount) {
		this.orderDetailsCount = orderDetailsCount;
	}

	@JsonProperty("unit_price")
	public String getUnitPrice() {
		return unitPrice;
	}

	@JsonProperty("unit_price")
	public void setUnitPrice(String unitPrice) {
		this.unitPrice = unitPrice;
	}

	@JsonProperty("quantity")
	public String getQuantity() {
		return quantity;
	}

	@JsonProperty("quantity")
	public void setQuantity(String quantity) {
		this.quantity = quantity;
	}

	@JsonProperty("discount")
	public String getDiscount() {
		return discount;
	}

	@JsonProperty("discount")
	public void setDiscount(String discount) {
		this.discount = discount;
	}

	@JsonProperty("product_name")
	public String getProductName() {
		return productName;
	}

	@JsonProperty("product_name")
	public void setProductName(String productName) {
		this.productName = productName;
	}

	@JsonProperty("supplied_id")
	public Object getSuppliedId() {
		return suppliedId;
	}

	@JsonProperty("supplied_id")
	public void setSuppliedId(Object suppliedId) {
		this.suppliedId = suppliedId;
	}

	@JsonProperty("category_id")
	public String getCategoryId() {
		return categoryId;
	}

	@JsonProperty("category_id")
	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
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