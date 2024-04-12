package com.epam.uni.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;

import static com.epam.uni.utils.Constants.SPARK_SESSION_MASTER_URL;
import static com.epam.uni.utils.TestConstants.PATH_TO_TEST_RECEIPT_RESTAURANTS_DATASET_DIRECTORY;
import static com.epam.uni.utils.TestConstants.PATH_TO_TEST_WEATHER_DATASET_DIRECTORY;
import static com.epam.uni.utils.TestConstants.SPARK_SESSION_TEST_APPLICATION_NAME;

public class TestBase {
    DataProcessor dataProcessor;
    DataReader dataReader;
    Dataset<Row> receipts;
    Dataset<Row> weather;

    @Before
    public void setUp() {
        SparkSession session = SparkSession.builder()
            .appName(SPARK_SESSION_TEST_APPLICATION_NAME)
            .master(SPARK_SESSION_MASTER_URL)
            .getOrCreate();

        dataProcessor = new DataProcessor();
        dataReader = new DataReader(session);

        receipts = dataReader
            .readReceiptsData(PATH_TO_TEST_RECEIPT_RESTAURANTS_DATASET_DIRECTORY);
        weather = dataReader
            .readWeatherData(PATH_TO_TEST_WEATHER_DATASET_DIRECTORY);
    }
}
