package it.okkam.flink.ckan.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CkanDatastoreSearch {

  @JsonProperty("resource_id")
  private String resourceId;

  @JsonProperty("total")
  private int rowCount;

  @JsonProperty("fields")
  private List<CkanField> fields;

  @JsonProperty("records")
  private List<Map<String, String>> records;

  public String getResourceId() {
    return resourceId;
  }

  public void setResourceId(String resourceId) {
    this.resourceId = resourceId;
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public List<CkanField> getFields() {
    return fields;
  }

  public void setFields(List<CkanField> fields) {
    this.fields = fields;
  }

  public List<Map<String, String>> getRecords() {
    return records;
  }

  public void setRecords(List<Map<String, String>> records) {
    this.records = records;
  }

}
