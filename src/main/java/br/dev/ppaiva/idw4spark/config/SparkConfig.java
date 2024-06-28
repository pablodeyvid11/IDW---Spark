package br.dev.ppaiva.idw4spark.config;

import java.io.Serializable;

import br.dev.ppaiva.idw4spark.models.enums.DataStructure;
import br.dev.ppaiva.idw4spark.models.enums.Environment;
import br.dev.ppaiva.idw4spark.models.enums.FileType;
import br.dev.ppaiva.idw4spark.models.enums.ProcessingMethod;

public class SparkConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	private DataStructure dataStructure;
	private Environment environment;
	private FileType fileType;
	private ProcessingMethod processingMethod;

	public SparkConfig() {
	}

	public SparkConfig(DataStructure dataStructure, Environment environment, FileType fileType,
			ProcessingMethod processingMethod) {
		this.dataStructure = dataStructure;
		this.environment = environment;
		this.fileType = fileType;
		this.processingMethod = processingMethod;
	}

	public DataStructure getDataStructure() {
		return dataStructure;
	}

	public void setDataStructure(DataStructure dataStructure) {
		this.dataStructure = dataStructure;
	}

	public Environment getEnvironment() {
		return environment;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType fileType) {
		this.fileType = fileType;
	}

	public ProcessingMethod getProcessingMethod() {
		return processingMethod;
	}

	public void setProcessingMethod(ProcessingMethod processingMethod) {
		this.processingMethod = processingMethod;
	}

}
