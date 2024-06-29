package br.dev.ppaiva.idw4spark;

import java.io.Serializable;

import br.dev.ppaiva.idw4spark.config.SparkConfig;
import br.dev.ppaiva.idw4spark.models.Point;
import br.dev.ppaiva.idw4spark.models.enums.DataStructure;
import br.dev.ppaiva.idw4spark.models.enums.Environment;
import br.dev.ppaiva.idw4spark.models.enums.FileType;
import br.dev.ppaiva.idw4spark.models.enums.ProcessingMethod;
import br.dev.ppaiva.idw4spark.processor.IDWSparkProcessor;
import br.dev.ppaiva.idw4spark.util.TimeTracker;

public class Idw4SparkApplication implements Serializable {
	private static final long serialVersionUID = 1L;

	// Dataset de 200mb
	private static String datasetPathTXT = System.getProperty("user.dir") + System.getProperty("file.separator")
			+ "dataset.txt";
	private static String datasetPathCSV = System.getProperty("user.dir") + System.getProperty("file.separator")
			+ "dataset.csv";
	private static String datasetPathPARQUET = System.getProperty("user.dir") + System.getProperty("file.separator")
			+ "dataset.parquet";

	// Dataset de 1gb
//	private static String datasetPathTXT = System.getProperty("user.dir") + System.getProperty("file.separator") + "dataset_1gb.txt";
//	private static String datasetPathCSV = System.getProperty("user.dir") + System.getProperty("file.separator") + "dataset_1gb.csv";
//	private static String datasetPathPARQUET = System.getProperty("user.dir") + System.getProperty("file.separator") + "dataset_1gb.parquet";

	private static TimeTracker tt = new TimeTracker();

	private static Point unknownPoint = new Point(34.0522, -118.2437, 0);

	public static void main(String[] args) {
		tt.reset();

		// RDD Local
		tt.start();
		SparkConfig rddLocalConfig = new SparkConfig(DataStructure.RDD, null, null, null);
		IDWSparkProcessor rddLocalProcessor = new IDWSparkProcessor(datasetPathTXT, rddLocalConfig);
		rddLocalProcessor.interpolate(unknownPoint);
		tt.end();
		System.out.println("RDD LOCAL: " + tt.getFormattedElapsedTime() + "\n");
		tt.reset();

		// Dataframe com CSV e SQL Local
		tt.start();
		SparkConfig dataframeCsvSQLLocalConfig = new SparkConfig(DataStructure.DATAFRAME, Environment.LOCAL,
				FileType.CSV, ProcessingMethod.SQL);
		IDWSparkProcessor dataframeCsvSQLLocalProcessor = new IDWSparkProcessor(datasetPathCSV,
				dataframeCsvSQLLocalConfig);
		dataframeCsvSQLLocalProcessor.interpolate(unknownPoint);
		tt.end();
		System.out.println("Dataframe com CSV e SQL Local: " + tt.getFormattedElapsedTime() + "\n");
		tt.reset();

		// Dataframe com CSV e Métodos Local
		tt.start();
		SparkConfig dataframeCsvMethodLocalConfig = new SparkConfig(DataStructure.DATAFRAME, Environment.LOCAL,
				FileType.CSV, ProcessingMethod.METHOD);
		IDWSparkProcessor dataframeCsvMethodLocalProcessor = new IDWSparkProcessor(datasetPathCSV,
				dataframeCsvMethodLocalConfig);
		dataframeCsvMethodLocalProcessor.interpolate(unknownPoint);
		tt.end();
		System.out.println("Dataframe com CSV e Method Local: " + tt.getFormattedElapsedTime() + "\n");
		tt.reset();

		// Dataframe com Parquet e SQL Local
		tt.start();
		SparkConfig dataframeParquetSQLLocalConfig = new SparkConfig(DataStructure.DATAFRAME, Environment.LOCAL,
				FileType.PARQUET, ProcessingMethod.SQL);
		IDWSparkProcessor dataframeParquetSQLLocalProcessor = new IDWSparkProcessor(datasetPathPARQUET,
				dataframeParquetSQLLocalConfig);
		dataframeParquetSQLLocalProcessor.interpolate(unknownPoint);
		tt.end();
		System.out.println("Dataframe com Parquet e SQL Local: " + tt.getFormattedElapsedTime() + "\n");
		tt.reset();

		// Dataframe com Parquet e Métodos Local
		tt.start();
		SparkConfig dataframeParquetMethodLocalConfig = new SparkConfig(DataStructure.DATAFRAME, Environment.LOCAL,
				FileType.PARQUET, ProcessingMethod.METHOD);
		IDWSparkProcessor dataframeParquetMethodLocalProcessor = new IDWSparkProcessor(datasetPathPARQUET,
				dataframeParquetMethodLocalConfig);
		dataframeParquetMethodLocalProcessor.interpolate(unknownPoint);
		tt.end();
		System.out.println("Dataframe com Parquet e Method Local: " + tt.getFormattedElapsedTime() + "\n");
		tt.reset();
	}

}