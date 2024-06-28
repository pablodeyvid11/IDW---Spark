package br.dev.ppaiva.idw4spark.models.enums;

import java.io.Serializable;

public enum DataStructure implements Serializable {

	RDD(1, "RDD"), DATAFRAME(2, "Dataframe");

	private final int id;
	private final String description;

	DataStructure(int id, String description) {
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
