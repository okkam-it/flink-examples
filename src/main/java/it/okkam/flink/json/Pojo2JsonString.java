package it.okkam.flink.json;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class Pojo2JsonString<T> extends RichMapFunction<T, String> {

  private static final long serialVersionUID = 1L;
	private ObjectMapper mapper = new ObjectMapper();

  @Override
  public String map(T value) throws Exception {
    return mapper.writeValueAsString(value);
  }

}