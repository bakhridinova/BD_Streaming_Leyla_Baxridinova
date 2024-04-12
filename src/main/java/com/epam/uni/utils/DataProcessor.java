package com.epam.uni.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.epam.uni.utils.Constants.CITY_COLUMN;
import static com.epam.uni.utils.Constants.COUNTRY_COLUMN;
import static com.epam.uni.utils.Constants.DATE_COLUMN;
import static com.epam.uni.utils.Constants.LATITUDE_COLUMN;
import static com.epam.uni.utils.Constants.LONGITUDE_COLUMN;
import static com.epam.uni.utils.Constants.WEATHER_DATE_COLUMN;
import static org.apache.spark.sql.functions.col;

public class DataProcessor {
    public Dataset<Row> enrichAndFilterData(Dataset<Row> receipts, Dataset<Row> weather) {
        Dataset<Row> enrichedReceipts = receipts.join(weather,
            receipts.col(DATE_COLUMN).equalTo(weather.col(WEATHER_DATE_COLUMN))
                .and(receipts.col(LATITUDE_COLUMN).equalTo(weather.col(LATITUDE_COLUMN)))
                .and(receipts.col(LONGITUDE_COLUMN).equalTo(weather.col(LONGITUDE_COLUMN)))
        );

        enrichedReceipts = enrichedReceipts.drop(
            DATE_COLUMN,
            CITY_COLUMN, COUNTRY_COLUMN,
            LATITUDE_COLUMN, LONGITUDE_COLUMN
        );

        return enrichedReceipts.filter(col("avg_tmpr_c").gt(0));
    }

    public Dataset<Row> calculateFields(Dataset<Row> data) {
        return data.withColumn("real_total_cost",
            col("total_cost")
                .minus(col("total_cost")
                    .multiply(col("discount")))
        );
    }

    public Dataset<Row> applyAdditionalLogic(Dataset<Row> data) {
        return data.withColumn("promo_cold_drinks",
            col("avg_tmpr_c").gt(25));
    }
}
