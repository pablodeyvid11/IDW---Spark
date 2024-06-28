package br.dev.ppaiva.idw4spark.models.enums;

import java.io.Serializable;

public enum ProcessingMethod implements Serializable {

	METHOD(1, "Method"), SQL(2, "SQL");

	private final int id;
	private final String description;

	ProcessingMethod(int id, String description) {
		this.id = id;
		this.description = description;
	}

	public String getDescription() {
		return description;
	}

	public int getId() {
		return id;
	}
}
