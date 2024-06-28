package br.dev.ppaiva.idw4spark.udf;

import java.io.Serializable;

import org.apache.spark.sql.api.java.UDF4;

public class HarvesineUDFDouble implements UDF4<Double, Double, Double, Double, Double>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public Double call(Double lon1, Double lat1, Double lon2, Double lat2) throws Exception {
		if (lon1 == null || lat1 == null || lon2 == null || lat2 == null) {
			return null;
		}
		final int R = 6371; // Radius of the earth
		double latDistance = Math.toRadians(lat2 - lat1);
		double lonDistance = Math.toRadians(lon2 - lon1);
		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1))
				* Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		return R * c;
	}
}
