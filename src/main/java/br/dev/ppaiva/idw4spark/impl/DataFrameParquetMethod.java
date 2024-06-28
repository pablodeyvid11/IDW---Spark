package br.dev.ppaiva.idw4spark.impl;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import br.dev.ppaiva.idw4spark.models.Point;
import br.dev.ppaiva.idw4spark.template.IDWSparkTemplate;
import br.dev.ppaiva.idw4spark.udf.HarvesineUDFDouble;

public class DataFrameParquetMethod extends IDWSparkTemplate implements Serializable {
	private static final long serialVersionUID = 1L;

	public DataFrameParquetMethod(SparkSession spark, String datasetPath) {
		super(spark, datasetPath);
	}

	@Override
	protected Point processData(Point unknownPoint) {
		Dataset<Row> data = spark.read().format("parquet").option("header", "true").option("inferSchema", "true")
                .load(datasetPath);

        spark.udf().register("harvesine", new HarvesineUDFDouble(), DataTypes.DoubleType);

        Dataset<Row> filteredData = data.filter("longitude IS NOT NULL AND latitude IS NOT NULL");

        Dataset<Row> unknownPointDF = spark.createDataFrame(java.util.Collections.singletonList(unknownPoint), Point.class)
                                           .withColumnRenamed("latitude", "unknown_latitude")
                                           .withColumnRenamed("longitude", "unknown_longitude")
                                           .withColumnRenamed("value", "unknown_value");

        Dataset<Row> crossJoined = filteredData.crossJoin(unknownPointDF);


        Dataset<Row> withDistances = crossJoined.withColumn("distance", callUDF("harvesine", col("longitude").cast(DataTypes.DoubleType),
                col("latitude").cast(DataTypes.DoubleType), col("unknown_longitude").cast(DataTypes.DoubleType), col("unknown_latitude").cast(DataTypes.DoubleType)));

        Dataset<Row> withInverseDistances = withDistances.withColumn("distInt", expr("1 / pow(distance, 2)"))
                .withColumn("weightedValue", col("distInt").multiply(col("value")));

 
        Dataset<Row> aggregated = withInverseDistances.agg(sum("weightedValue").alias("sumWeightedValues"),
                sum("distInt").alias("sumInverseDistances"));

        Dataset<Row> interpolated = aggregated
                .withColumn("latitude", lit(unknownPoint.getLatitude()))
                .withColumn("longitude", lit(unknownPoint.getLongitude()))
                .withColumn("interpolated_value", col("sumWeightedValues").divide(col("sumInverseDistances")))
                .select("latitude", "longitude", "interpolated_value");

        double interpolatedValue = interpolated.first().getDouble(2);

        spark.stop();
        return new Point(unknownPoint.getLatitude(), unknownPoint.getLongitude(), interpolatedValue);
	}
}
