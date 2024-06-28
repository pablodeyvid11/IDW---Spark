package br.dev.ppaiva.idw4spark;

import java.io.Serializable;

import br.dev.ppaiva.idw4spark.config.SparkConfig;
import br.dev.ppaiva.idw4spark.models.Point;
import br.dev.ppaiva.idw4spark.models.enums.DataStructure;
import br.dev.ppaiva.idw4spark.processor.IDWSparkProcessor;
import br.dev.ppaiva.idw4spark.util.TimeTracker;

public class Idw4SparkApplication implements Serializable {
	private static final long serialVersionUID = 1L;

	private static String datasetPathTXT = System.getProperty("user.dir") + System.getProperty("file.separator")
			+ "dataset.txt";
	private static String datasetPathCSV = System.getProperty("user.dir") + System.getProperty("file.separator")
			+ "dataset.csv";
	private static String datasetPathPARQUET = System.getProperty("user.dir") + System.getProperty("file.separator")
			+ "dataset.parquet";

	private static TimeTracker tt = new TimeTracker();

	private static Point unknownPoint = new Point(34.0522, -118.2437, 0);

	public static void main(String[] args) {
		tt.reset();
		tt.start();
		SparkConfig rddLocalConfig = new SparkConfig(DataStructure.RDD, null, null, null);
		IDWSparkProcessor rddLocalProcessor = new IDWSparkProcessor(datasetPathTXT, rddLocalConfig);
		Point rddLocalPointInterpolatted = rddLocalProcessor.interpolate(unknownPoint);
		tt.end();
		System.out.println("RDD LOCAL: " + rddLocalPointInterpolatted.toString());
		System.out.println("RDD LOCAL: " + tt.getFormattedElapsedTime() + "\n");
		tt.reset();
	}
}