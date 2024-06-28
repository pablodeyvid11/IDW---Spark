package br.dev.ppaiva.idw4spark.udf;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.sql.api.java.UDF4;


public class HarvesineUDFBigDecimal implements UDF4<BigDecimal, BigDecimal, BigDecimal, BigDecimal, Double>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public Double call(BigDecimal lon1, BigDecimal lat1, BigDecimal lon2, BigDecimal lat2) throws Exception {
        if (lon1 == null || lat1 == null || lon2 == null || lat2 == null) {
            return null;
        }
        final int R = 6371;
        double lon1Double = lon1.doubleValue();
        double lat1Double = lat1.doubleValue();
        double lon2Double = lon2.doubleValue();
        double lat2Double = lat2.doubleValue();
        double latDistance = Math.toRadians(lat2Double - lat1Double);
        double lonDistance = Math.toRadians(lon2Double - lon1Double);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1Double)) * Math.cos(Math.toRadians(lat2Double))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}
