package it.okkam.flink.json;

public class TestPojo {

	private String name;
	private int age;

	public String getName() {
		return name;
	}

	public TestPojo setName(String name) {
		this.name = name;
		return this;
	}

	public int getAge() {
		return age;
	}

	public TestPojo setAge(int age) {
		this.age = age;
		return this;
	}

	@Override
	public String toString() {
		return "name=" + name + ", age=" + age;
	}
}
