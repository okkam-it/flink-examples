package it.okkam.flink.ckan.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class CkanError {

  private String message;

  /**
   * actually the original is __type.
   */
  private String type;

  /**
   * Holds fields we can't foresee.
   */
  private Map<String, Object> others = new HashMap<>();

  @Override
  public String toString() {
    return "Ckan error of type: " + getType() + "  message:" + getMessage() + "  Other fields:"
        + others.toString();
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  /**
   * For types, see {@link CkanError class description}.
   */
  @JsonProperty("__type")
  public String getType() {
    return type;
  }

  /**
   * What are possible types..?.
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Holds fields we can't foresee.
   */
  @JsonAnyGetter
  public Map<String, Object> getOthers() {
    return others;
  }

  /**
   * Holds fields we can't foresee.
   */
  @JsonAnySetter
  public void setOthers(String name, Object value) {
    others.put(name, value);
  }
}
