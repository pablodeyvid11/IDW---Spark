package br.dev.ppaiva.idw4spark.impl;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import br.dev.ppaiva.idw4spark.models.Point;
import br.dev.ppaiva.idw4spark.template.IDWSparkTemplate;

public class RDDLocal extends IDWSparkTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    public RDDLocal(SparkSession spark, String datasetPath) {
        super(spark, datasetPath);
    }

    @Override
    protected Point processData(Point unknownPoint) {
        try (JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
            JavaRDD<String> data = sc.textFile(datasetPath);

            JavaRDD<Point> pointsRDD = data.map(line -> {
                String[] parts = line.split(",");
                return new Point(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]),
                        Double.parseDouble(parts[2]));
            });

            double suminf = pointsRDD.mapToDouble(point -> {
                double distance = harvesine(point.getLongitude(), point.getLatitude(), unknownPoint.getLongitude(),
                        unknownPoint.getLatitude());
                return 1 / Math.pow(distance, 2);
            }).sum();

            double sumsupTotal = pointsRDD.mapToDouble(point -> {
                double distance = harvesine(point.getLongitude(), point.getLatitude(), unknownPoint.getLongitude(),
                        unknownPoint.getLatitude());
                return (1 / Math.pow(distance, 2)) * point.getValue();
            }).sum();
            
            
            double valueInterpolated = sumsupTotal / suminf;
            spark.stop();
            return new Point(unknownPoint.getLatitude(), unknownPoint.getLongitude(), valueInterpolated);
        }
    }
}
