package it.okkam.flink;

import it.okkam.flink.json.JsonStringToPojo;
import it.okkam.flink.json.Pojo2JsonString;
import it.okkam.flink.json.TestPojo;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class Json2PojoExample {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TestPojo pojo1 = new TestPojo().setName("test1").setAge(11);
		TestPojo pojo2 = new TestPojo().setName("test2").setAge(22);
		TestPojo pojo3 = new TestPojo().setName("test3").setAge(33);
		List<TestPojo> list = Arrays.asList(pojo1, pojo2, pojo3);

		final DataSet<TestPojo> ds = env.fromCollection(list);
		final List<TestPojo> converted = ds//
				.map(new Pojo2JsonString<TestPojo>())//
				.map(new JsonStringToPojo<TestPojo>(TestPojo.class))//
				.returns(TypeExtractor.getForClass(TestPojo.class))//
				.collect();
		System.out.println("Source array: " + Arrays.deepToString(list.toArray()));
		System.out.println("Target array: " + Arrays.deepToString(converted.toArray()));
	}
}
