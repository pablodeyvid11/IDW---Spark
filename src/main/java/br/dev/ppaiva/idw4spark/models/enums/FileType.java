package br.dev.ppaiva.idw4spark.models.enums;

import java.io.Serializable;

public enum FileType implements Serializable {

	CSV(1, "CSV"), PARQUET(2, "Parquet"), TXT(3, "Text File");

	private final int id;
	private final String description;

	FileType(int id, String description) {
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
