package br.dev.ppaiva.idw4spark.template;

import java.io.Serializable;

import org.apache.spark.sql.SparkSession;

import br.dev.ppaiva.idw4spark.models.Point;

public abstract class IDWSparkTemplate implements Serializable {
	private static final long serialVersionUID = 1L;

	protected SparkSession spark;
	protected String datasetPath;

	public IDWSparkTemplate(SparkSession spark, String datasetPath) {
		this.spark = spark;
		this.datasetPath = datasetPath;
	}

	public final Point process(Point unknownPoint) {
		return processData(unknownPoint);
	}

	protected abstract Point processData(Point unknownPoint);

	public double harvesine(double lon1, double lat1, double lon2, double lat2) {
		final int R = 6371;
		double latDistance = Math.toRadians(lat2 - lat1);
		double lonDistance = Math.toRadians(lon2 - lon1);
		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1))
				* Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		return R * c;
	}
}
