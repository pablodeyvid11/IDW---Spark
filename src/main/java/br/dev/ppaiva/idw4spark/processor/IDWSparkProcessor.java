package br.dev.ppaiva.idw4spark.processor;

import java.io.Serializable;

import org.apache.spark.sql.SparkSession;

import br.dev.ppaiva.idw4spark.config.SparkConfig;
import br.dev.ppaiva.idw4spark.impl.DataFrameCSVMethod;
import br.dev.ppaiva.idw4spark.impl.DataFrameCSVSQL;
import br.dev.ppaiva.idw4spark.impl.DataFrameParquetMethod;
import br.dev.ppaiva.idw4spark.impl.DataFrameParquetSQL;
import br.dev.ppaiva.idw4spark.impl.RDDLocal;
import br.dev.ppaiva.idw4spark.models.Point;
import br.dev.ppaiva.idw4spark.models.enums.DataStructure;
import br.dev.ppaiva.idw4spark.models.enums.Environment;
import br.dev.ppaiva.idw4spark.models.enums.FileType;
import br.dev.ppaiva.idw4spark.models.enums.ProcessingMethod;
import br.dev.ppaiva.idw4spark.template.IDWSparkTemplate;

public class IDWSparkProcessor implements Serializable {
	private static final long serialVersionUID = 1L;

	private String datasetPath;
	private SparkConfig sparkConfig;
	private IDWSparkTemplate spark;

	public IDWSparkProcessor(String datasetPath, SparkConfig sparkConfig) {
		this.datasetPath = datasetPath;
		this.sparkConfig = sparkConfig;
		initializeSparkTemplate();
	}

	private void initializeSparkTemplate() {

		if (sparkConfig.getDataStructure().equals(DataStructure.RDD)) {
			SparkSession sparkSession = SparkSession.builder().appName("RDD Local").master("local[*]").getOrCreate();
			this.spark = new RDDLocal(sparkSession, datasetPath);
			return;
		}

		if (sparkConfig.getDataStructure().equals(DataStructure.DATAFRAME)
				&& sparkConfig.getFileType().equals(FileType.CSV)
				&& sparkConfig.getProcessingMethod().equals(ProcessingMethod.SQL)
				&& sparkConfig.getEnvironment().equals(Environment.LOCAL)) {

			SparkSession sparkSession = SparkSession.builder().appName("Dataframe com CSV e SQL Local")
					.master("local[*]").getOrCreate();

			this.spark = new DataFrameCSVSQL(sparkSession, datasetPath);
			return;
		}

		if (sparkConfig.getDataStructure().equals(DataStructure.DATAFRAME)
				&& sparkConfig.getFileType().equals(FileType.CSV)
				&& sparkConfig.getProcessingMethod().equals(ProcessingMethod.METHOD)
				&& sparkConfig.getEnvironment().equals(Environment.LOCAL)) {

			SparkSession sparkSession = SparkSession.builder().appName("Dataframe com CSV e Métodos Local")
					.master("local[*]").getOrCreate();

			this.spark = new DataFrameCSVMethod(sparkSession, datasetPath);
			return;
		}

		if (sparkConfig.getDataStructure().equals(DataStructure.DATAFRAME)
				&& sparkConfig.getFileType().equals(FileType.PARQUET)
				&& sparkConfig.getProcessingMethod().equals(ProcessingMethod.SQL)
				&& sparkConfig.getEnvironment().equals(Environment.LOCAL)) {

			SparkSession sparkSession = SparkSession.builder().appName("Dataframe com Parquet e SQL Local")
					.master("local[*]").getOrCreate();

			this.spark = new DataFrameParquetSQL(sparkSession, datasetPath);
			return;
		}

		if (sparkConfig.getDataStructure().equals(DataStructure.DATAFRAME)
				&& sparkConfig.getFileType().equals(FileType.PARQUET)
				&& sparkConfig.getProcessingMethod().equals(ProcessingMethod.METHOD)
				&& sparkConfig.getEnvironment().equals(Environment.LOCAL)) {

			SparkSession sparkSession = SparkSession.builder().appName("Dataframe com Parquet e Métodos Local")
					.master("local[*]").getOrCreate();

			this.spark = new DataFrameParquetMethod(sparkSession, datasetPath);
			return;
		}
		
		throw new IllegalStateException("SparkConfig is in an unsupported state.");
	}

	public Point interpolate(Point unknownPoint) {
		return this.spark.process(unknownPoint);
	}
}