package it.okkam.flink.json;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonStringToPojo<T> extends RichMapFunction<String, T> {
	private static final long serialVersionUID = 1L;
	private final Class<T> targetClass;
	private ObjectMapper mapper = new ObjectMapper();

	public JsonStringToPojo(Class<T> targetClass) {
		this.targetClass = targetClass;
	}

	@Override
	public T map(String value) throws Exception {
		return mapper.readValue(value, targetClass);
	}
}