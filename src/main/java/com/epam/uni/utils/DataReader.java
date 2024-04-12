package com.epam.uni.utils;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static com.epam.uni.utils.Constants.CHECKPOINT_LOCATION_OPTION;
import static com.epam.uni.utils.Constants.CITY;
import static com.epam.uni.utils.Constants.COUNTRY;
import static com.epam.uni.utils.Constants.CSV;
import static com.epam.uni.utils.Constants.DATE;
import static com.epam.uni.utils.Constants.DATE_FORMAT;
import static com.epam.uni.utils.Constants.DATE_TIME;
import static com.epam.uni.utils.Constants.DATE_TIME_FORMAT;
import static com.epam.uni.utils.Constants.HEADER_OPTION;
import static com.epam.uni.utils.Constants.TRUE;
import static com.epam.uni.utils.Constants.LATITUDE;
import static com.epam.uni.utils.Constants.LONGITUDE;
import static com.epam.uni.utils.Constants.OUTPUT_MODE;
import static com.epam.uni.utils.Constants.PATH_OPTION;
import static com.epam.uni.utils.Constants.RECEIPT;
import static com.epam.uni.utils.Constants.WEATHER;
import static com.epam.uni.utils.Constants.WEATHER_DATE;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.year;

@RequiredArgsConstructor
public class DataReader {
    private static final String TIMEOUT_MESSAGE =
        "Timeout occurred while waiting for streaming query to terminate";
    private static final String STREAMING_QUERY_MESSAGE =
        "An error occurred while processing the streaming query";

    private final SparkSession session;

    /**
     * <p>
     * The method reads receipt data from a given path for the year 2022.
     * It loads the data in CSV format and applies several transformations:
     * <br>
     * - It adds a prefix to the date, lat, lng, country, and city columns to distinguish them from similar columns in other datasets.
     * <br>
     * - It converts the date_time column to a date format and drops the original column as it's no longer needed after extraction.
     * <br>
     * - It rounds the lat and lng columns to 2 decimal places.
     * </p>
     * @param path The path to the receipt data.
     * @return The formatted receipt data.
     */

    public Dataset<Row> readReceiptsDataFor2022(String path) {
        return session.read().format(CSV)
            .option(HEADER_OPTION, TRUE)
            .load(path)
            .withColumn(RECEIPT + DATE,
                to_date(to_timestamp(col(DATE_TIME), DATE_TIME_FORMAT), DATE_FORMAT))
            .withColumn(LATITUDE, round(col(LATITUDE), 2))
            .withColumn(LONGITUDE, round(col(LONGITUDE), 2))
            .withColumnRenamed(LATITUDE, RECEIPT + LATITUDE)
            .withColumnRenamed(LONGITUDE, RECEIPT + LONGITUDE)
            .withColumnRenamed(COUNTRY, RECEIPT + COUNTRY)
            .withColumnRenamed(CITY, RECEIPT + CITY)
            .drop(DATE_TIME)
            .filter(year(col(RECEIPT + DATE)).equalTo(2022));
    }

    /**
     * <p>
     * The method reads weather data from a given path for the year 2022.
     * It loads the data in CSV format and applies several transformations:
     * <br>
     * - It adds a prefix to the date, lat, lng, country, and city columns to distinguish them from similar columns in other datasets.
     * <br>
     * - It converts the weather_date column to a date format and drops the original column as it's no longer needed after extraction.
     * <br>
     * - It rounds the lat and lng columns to 2 decimal places.
     * </p>
     * @param path The path to the weather data.
     * @return The formatted weather data.
     */
    public Dataset<Row> readWeatherDataFor2022(String path) {
        return session.read().format(CSV)
            .option(HEADER_OPTION, TRUE)
            .load(path)
            .withColumn(WEATHER + DATE,
                to_date(col(WEATHER_DATE), DATE_FORMAT))
            .withColumn(LATITUDE, round(col(LATITUDE), 2))
            .withColumn(LONGITUDE, round(col(LONGITUDE), 2))
            .withColumnRenamed(LATITUDE, WEATHER + LATITUDE)
            .withColumnRenamed(LONGITUDE, WEATHER + LONGITUDE)
            .withColumnRenamed(COUNTRY, WEATHER + COUNTRY)
            .withColumnRenamed(CITY, WEATHER + CITY)
            .drop(WEATHER_DATE)
            .filter(year(col(WEATHER + DATE)).equalTo(2022));
    }

    /**
     * The method reads streaming data from a given path using a provided schema.
     *
     * @param schema The schema to use when reading the data.
     * @param path The path to the streaming data.
     * @return The streaming data.
     */
    public Dataset<Row> readStream(StructType schema, String path) {
        return session.readStream().format(CSV)
            .option(HEADER_OPTION, TRUE)
            .schema(schema)
            .load(path);
    }

    /**
     * The method writes a given dataset to a specified path using a streaming query.
     * It starts the query and waits for it to terminate.
     * If a timeout or a streaming query exception occurs, it throws a runtime exception.
     *
     * @param data The data to write.
     * @param path The path to write the data to.
     * @param checkpointLocation The checkpoint location for the streaming query.
     */
    public void writeStream(Dataset<Row> data, String path, String checkpointLocation) {
        try {
            StreamingQuery query = data.writeStream()
                .outputMode(OUTPUT_MODE)
                .format(CSV)
                .option(HEADER_OPTION, TRUE)
                .option(PATH_OPTION, path)
                .option(CHECKPOINT_LOCATION_OPTION, checkpointLocation)
                .start();

            query.awaitTermination();
        } catch (TimeoutException e) {
            throw new RuntimeException(TIMEOUT_MESSAGE, e);
        } catch (StreamingQueryException e) {
            throw new RuntimeException(STREAMING_QUERY_MESSAGE, e);
        }
    }
}
