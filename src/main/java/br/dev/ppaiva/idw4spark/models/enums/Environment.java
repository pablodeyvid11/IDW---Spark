package br.dev.ppaiva.idw4spark.models.enums;

import java.io.Serializable;

public enum Environment implements Serializable {

	LOCAL(1, "Local"), KUBERNETES(2, "Kubernetes");

	private final int id;
	private final String description;

	Environment(int id, String description) {
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
