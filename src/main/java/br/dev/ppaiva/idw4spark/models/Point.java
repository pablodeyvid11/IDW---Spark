package br.dev.ppaiva.idw4spark.models;

import java.io.Serializable;

public class Point implements Serializable {
	private static final long serialVersionUID = 1L;

	private double latitude;
	private double longitude;
	private double value;

	public Point(double latitude, double longitude, double value) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.value = value;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public double getValue() {
		return value;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Point [latitude=");
		builder.append(latitude);
		builder.append(", longitude=");
		builder.append(longitude);
		builder.append(", value=");
		builder.append(value);
		builder.append("]");
		return builder.toString();
	}

}