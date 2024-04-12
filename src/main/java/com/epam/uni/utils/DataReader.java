package com.epam.uni.utils;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static com.epam.uni.utils.Constants.*;
import static org.apache.spark.sql.functions.*;

@RequiredArgsConstructor
public class DataReader {
    private final SparkSession session;

    public Dataset<Row> readReceiptsData(String path) {
        return session.read().format(CSV)
            .option("header", "true")
            .load(path)
            .withColumn(DATE_COLUMN, to_date(to_timestamp(col(DATE_TIME_COLUMN), DATE_TIME_FORMAT), DATE_FORMAT))
            .withColumn(LATITUDE_COLUMN, round(col(LATITUDE_COLUMN), 2))
            .withColumn(LONGITUDE_COLUMN, round(col(LONGITUDE_COLUMN), 2));
    }

    public Dataset<Row> readWeatherData(String path) {
        return session.read().format(CSV)
            .option("header", "true")
            .load(path)
            .withColumn(DATE_COLUMN, to_date(to_timestamp(col(WEATHER_DATE_COLUMN), DATE_TIME_FORMAT), DATE_FORMAT))
            .withColumn(LATITUDE_COLUMN, round(col(LATITUDE_COLUMN), 2))
            .withColumn(LONGITUDE_COLUMN, round(col(LONGITUDE_COLUMN), 2));
    }

    public Dataset<Row> readStream(StructType schema, String path) {
        return session.readStream().format(CSV)
            .option("header", "true")
            .schema(schema)
            .load(path);
    }

    public void writeStream(Dataset<Row> data, String path, String checkpointLocation) {
        try {
            StreamingQuery query = data.writeStream()
                .outputMode("append")
                .format(CSV)
                .option("path", path)
                .option("checkpointLocation", checkpointLocation)
                .start();

            query.awaitTermination();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }
}
