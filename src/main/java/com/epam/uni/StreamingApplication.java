package com.epam.uni;

import com.epam.uni.utils.DataProcessor;
import com.epam.uni.utils.DataReader;
import org.apache.spark.sql.SparkSession;

import static com.epam.uni.utils.Constants.PATH_TO_CHECKPOINT_LOCATION;
import static com.epam.uni.utils.Constants.PATH_TO_OUTPUT_DIRECTORY;
import static com.epam.uni.utils.Constants.PATH_TO_RECEIPT_RESTAURANTS_DATASET_DIRECTORY;
import static com.epam.uni.utils.Constants.PATH_TO_WEATHER_DATASET_DIRECTORY;
import static com.epam.uni.utils.Constants.SPARK_SESSION_APPLICATION_NAME;
import static com.epam.uni.utils.Constants.SPARK_SESSION_MASTER_URL;

public class StreamingApplication {
    private final DataReader dataReader;
    private final DataProcessor dataProcessor;

    public StreamingApplication() {
        SparkSession session = SparkSession.builder()
            .appName(SPARK_SESSION_APPLICATION_NAME)
            .master(SPARK_SESSION_MASTER_URL)
            .getOrCreate();

        dataReader = new DataReader(session);
        dataProcessor = new DataProcessor();
    }

    public static void main(String[] args) {
        new StreamingApplication().run();
    }

    private void run() {
        var staticReceipts = dataReader
            .readReceiptsData(PATH_TO_RECEIPT_RESTAURANTS_DATASET_DIRECTORY);
        var staticWeather = dataReader
            .readWeatherData(PATH_TO_WEATHER_DATASET_DIRECTORY);

        var enrichedReceipts = dataProcessor
            .enrichAndFilterData(staticReceipts, staticWeather);
        var calculatedReceipts = dataProcessor
            .calculateFields(enrichedReceipts);
        calculatedReceipts.write()
            .csv(PATH_TO_OUTPUT_DIRECTORY);

        var streamingData = dataReader.readStream(
            calculatedReceipts.schema(),
            PATH_TO_RECEIPT_RESTAURANTS_DATASET_DIRECTORY
        );
        var processedData = dataProcessor
            .applyAdditionalLogic(streamingData);

        dataReader.writeStream(
            processedData,
            PATH_TO_OUTPUT_DIRECTORY,
            PATH_TO_CHECKPOINT_LOCATION
        );
    }
}
