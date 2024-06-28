package br.dev.ppaiva.idw4spark.impl;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import br.dev.ppaiva.idw4spark.models.Point;
import br.dev.ppaiva.idw4spark.template.IDWSparkTemplate;
import br.dev.ppaiva.idw4spark.udf.HarvesineUDFBigDecimal;

public class DataFrameParquetSQL extends IDWSparkTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    public DataFrameParquetSQL(SparkSession spark, String datasetPath) {
        super(spark, datasetPath);
    }

    @Override
    protected Point processData(Point unknownPoint) {
        Dataset<Row> data = spark.read().format("parquet").option("header", "true").option("inferSchema", "true")
                .load(datasetPath);

        // Registrar a UDF harvesine
        spark.udf().register("harvesine", new HarvesineUDFBigDecimal(), DataTypes.DoubleType);

        // Remover linhas onde longitude ou latitude são nulos
        Dataset<Row> filteredData = data.filter("longitude IS NOT NULL AND latitude IS NOT NULL");

        // Criar uma tabela temporária para os dados
        filteredData.createOrReplaceTempView("data");

        // Calcular a distância e os termos necessários para a interpolação usando SQL
        Dataset<Row> withDistances = spark.sql(
                "SELECT *, " +
                "harvesine(CAST(longitude AS DECIMAL(10, 8)), CAST(latitude AS DECIMAL(10, 8)), " + unknownPoint.getLongitude() + ", " + unknownPoint.getLatitude() + ") as distance " +
                "FROM data"
        );

        withDistances.createOrReplaceTempView("withDistances");

        Dataset<Row> withInverseDistances = spark.sql(
                "SELECT *, " +
                "(1 / POW(distance, 2)) as distInt, " +
                "(1 / POW(distance, 2)) * value as weightedValue " +
                "FROM withDistances"
        );

        withInverseDistances.createOrReplaceTempView("withInverseDistances");

        // Realizar a agregação para calcular o valor interpolado
        Dataset<Row> aggregated = spark.sql(
                "SELECT " +
                "SUM(weightedValue) as sumWeightedValues, " +
                "SUM(distInt) as sumInverseDistances " +
                "FROM withInverseDistances"
        );

        Dataset<Row> interpolated = aggregated.withColumn("latitude", lit(unknownPoint.getLatitude()))
                .withColumn("longitude", lit(unknownPoint.getLongitude()))
                .withColumn("interpolated_value", col("sumWeightedValues").divide(col("sumInverseDistances")))
                .select("latitude", "longitude", "interpolated_value");

        
        
        double interpolatedValue = interpolated.first().getDouble(2);
        
        spark.stop();
        return new Point(unknownPoint.getLatitude(), unknownPoint.getLongitude(), interpolatedValue);
    }
}
