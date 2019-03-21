package spark.marklogic.credit;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
"Loan_ID",
"Gender",
"Married",
"Dependents",
"Education",
"Self_Employed",
"ApplicantIncome",
"CoapplicantIncome",
"LoanAmount",
"Loan_Amount_Term",
"Credit_History",
"Property_Area",
"Loan_Status"
})
public class Loan {

@JsonProperty("Loan_ID")
private String loanID;
@JsonProperty("Gender")
private String gender;
@JsonProperty("Married")
private String married;
@JsonProperty("Dependents")
private String dependents;
@JsonProperty("Education")
private String education;
@JsonProperty("Self_Employed")
private String selfEmployed;
@JsonProperty("ApplicantIncome")
private String applicantIncome;
@JsonProperty("CoapplicantIncome")
private String coapplicantIncome;
@JsonProperty("LoanAmount")
private String loanAmount;
@JsonProperty("Loan_Amount_Term")
private String loanAmountTerm;
@JsonProperty("Credit_History")
private String creditHistory;
@JsonProperty("Property_Area")
private String propertyArea;
@JsonProperty("Loan_Status")
private String loanStatus;
@JsonIgnore
private Map<String, Object> additionalProperties = new HashMap<String, Object>();

@JsonProperty("Loan_ID")
public String getLoanID() {
return loanID;
}

@JsonProperty("Loan_ID")
public void setLoanID(String loanID) {
this.loanID = loanID;
}

@JsonProperty("Gender")
public String getGender() {
return gender;
}

@JsonProperty("Gender")
public void setGender(String gender) {
this.gender = gender;
}

@JsonProperty("Married")
public String getMarried() {
return married;
}

@JsonProperty("Married")
public void setMarried(String married) {
this.married = married;
}

@JsonProperty("Dependents")
public String getDependents() {
return dependents;
}

@JsonProperty("Dependents")
public void setDependents(String dependents) {
this.dependents = dependents;
}

@JsonProperty("Education")
public String getEducation() {
return education;
}

@JsonProperty("Education")
public void setEducation(String education) {
this.education = education;
}

@JsonProperty("Self_Employed")
public String getSelfEmployed() {
return selfEmployed;
}

@JsonProperty("Self_Employed")
public void setSelfEmployed(String selfEmployed) {
this.selfEmployed = selfEmployed;
}

@JsonProperty("ApplicantIncome")
public String getApplicantIncome() {
return applicantIncome;
}

@JsonProperty("ApplicantIncome")
public void setApplicantIncome(String applicantIncome) {
this.applicantIncome = applicantIncome;
}

@JsonProperty("CoapplicantIncome")
public String getCoapplicantIncome() {
return coapplicantIncome;
}

@JsonProperty("CoapplicantIncome")
public void setCoapplicantIncome(String coapplicantIncome) {
this.coapplicantIncome = coapplicantIncome;
}

@JsonProperty("LoanAmount")
public String getLoanAmount() {
return loanAmount;
}

@JsonProperty("LoanAmount")
public void setLoanAmount(String loanAmount) {
this.loanAmount = loanAmount;
}

@JsonProperty("Loan_Amount_Term")
public String getLoanAmountTerm() {
return loanAmountTerm;
}

@JsonProperty("Loan_Amount_Term")
public void setLoanAmountTerm(String loanAmountTerm) {
this.loanAmountTerm = loanAmountTerm;
}

@JsonProperty("Credit_History")
public String getCreditHistory() {
return creditHistory;
}

@JsonProperty("Credit_History")
public void setCreditHistory(String creditHistory) {
this.creditHistory = creditHistory;
}

@JsonProperty("Property_Area")
public String getPropertyArea() {
return propertyArea;
}

@JsonProperty("Property_Area")
public void setPropertyArea(String propertyArea) {
this.propertyArea = propertyArea;
}

@JsonProperty("Loan_Status")
public String getLoanStatus() {
return loanStatus;
}

@JsonProperty("Loan_Status")
public void setLoanStatus(String loanStatus) {
this.loanStatus = loanStatus;
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