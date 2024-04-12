package com.epam.uni.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;

import static com.epam.uni.utils.Constants.AVERAGE_TEMPERATURE;
import static com.epam.uni.utils.Constants.CITY;
import static com.epam.uni.utils.Constants.COUNTRY;
import static com.epam.uni.utils.Constants.DATE;
import static com.epam.uni.utils.Constants.DISCOUNT;
import static com.epam.uni.utils.Constants.FALSE;
import static com.epam.uni.utils.Constants.FRANCHISE_ID;
import static com.epam.uni.utils.Constants.FRANCHISE_NAME;
import static com.epam.uni.utils.Constants.ID;
import static com.epam.uni.utils.Constants.ITEMS_COUNT;
import static com.epam.uni.utils.Constants.LATITUDE;
import static com.epam.uni.utils.Constants.LONGITUDE;
import static com.epam.uni.utils.Constants.MOST_POPULAR_ORDER_TYPE;
import static com.epam.uni.utils.Constants.ORDER_SIZE;
import static com.epam.uni.utils.Constants.PROMO_COLD_DRINKS;
import static com.epam.uni.utils.Constants.REAL_TOTAL_COST;
import static com.epam.uni.utils.Constants.RECEIPT;
import static com.epam.uni.utils.Constants.RESTAURANT;
import static com.epam.uni.utils.Constants.TOTAL_COST;
import static com.epam.uni.utils.Constants.TRUE;
import static com.epam.uni.utils.Constants.WEATHER;
import static com.epam.uni.utils.OrderSize.ERRONEOUS;
import static com.epam.uni.utils.OrderSize.LARGE;
import static com.epam.uni.utils.OrderSize.MEDIUM;
import static com.epam.uni.utils.OrderSize.SMALL;
import static com.epam.uni.utils.OrderSize.TINY;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

public class DataProcessor {
    /**
     * <p>
     * The method joins two datasets, receipts and weather,
     * on multiple columns (date, latitude, longitude, country, city).
     * After joining, it drops duplicate columns from the `weather` dataset and
     * removes receipt_ prefix from the remaining columns.
     * It then returns filtered out rows where the average temperature is less than or equal to 0.
     * </p>
     * @param receipts The dataset containing receipt data.
     * @param weather The dataset containing weather data.
     * @return The joined and filtered dataset.
     */
    public Dataset<Row> joinAndFilterData(Dataset<Row> receipts, Dataset<Row> weather) {
        Dataset<Row> joinData = receipts.join(weather,
            receipts.col(RECEIPT + DATE).equalTo(weather.col(WEATHER + DATE))
                .and(receipts.col(RECEIPT + LATITUDE)
                    .equalTo(weather.col(WEATHER + LATITUDE)))
                .and(receipts.col(RECEIPT + LONGITUDE)
                    .equalTo(weather.col(WEATHER + LONGITUDE)))
                .and(receipts.col(RECEIPT + COUNTRY)
                    .equalTo(weather.col(WEATHER + COUNTRY)))
                .and(receipts.col(RECEIPT + CITY)
                    .equalTo(weather.col(WEATHER + CITY)))
        );

        var columnsToDrop = new String[] {
            WEATHER + DATE,
            WEATHER + LATITUDE, WEATHER + LONGITUDE,
            WEATHER + COUNTRY, WEATHER + CITY
        };
        joinData = joinData.drop(columnsToDrop)
            .withColumnRenamed(RECEIPT + LATITUDE, LATITUDE)
            .withColumnRenamed(RECEIPT + LONGITUDE, LONGITUDE)
            .withColumnRenamed(RECEIPT + COUNTRY, COUNTRY)
            .withColumnRenamed(RECEIPT + CITY, CITY)
            .withColumnRenamed(RECEIPT + DATE, DATE);

        joinData = joinData
            .filter(col(AVERAGE_TEMPERATURE).gt(0));

        return joinData;
    }

    /**
     * <p>
     * The method calculates additional fields for the input dataset data.
     * It first calculates the real total cost by subtracting the discount from the total cost.
     * Then, it groups the data by franchise id and id, and aggregates various fields.
     * It also calculates the order size based on the items count.
     * It determines the most popular order type for each franchise id and date before retuning rows back.
     * </p>
     * @param data The input dataset.
     * @return The dataset with additional calculated fields.
     */
    public Dataset<Row> calculateFields(Dataset<Row> data) {
        data = data.withColumn(REAL_TOTAL_COST,
            col(TOTAL_COST)
                .minus(col(TOTAL_COST)
                    .multiply(col(DISCOUNT)))
        );

        data = data
            .groupBy(RESTAURANT + FRANCHISE_ID, RECEIPT + ID)
            .agg(
                first(col(ID)).as(ID),
                first(col(FRANCHISE_ID)).as(FRANCHISE_ID),
                first(col(FRANCHISE_NAME)).as(FRANCHISE_NAME),
                first(col(COUNTRY)).as(COUNTRY),
                first(col(CITY)).as(CITY),
                first(col(LATITUDE)).as(LATITUDE),
                first(col(LONGITUDE)).as(LONGITUDE),
                first(col(DATE)).as(DATE),
                first(col(AVERAGE_TEMPERATURE)).as(AVERAGE_TEMPERATURE),
                first(col(TOTAL_COST)).as(TOTAL_COST),
                first(col(DISCOUNT)).as(DISCOUNT),
                first(col(REAL_TOTAL_COST)).as(REAL_TOTAL_COST),
                count("*").as(ITEMS_COUNT)
            )
            .withColumn(ORDER_SIZE,
                when(col(ITEMS_COUNT).isNull().or(col(ITEMS_COUNT).leq(ERRONEOUS.getOrderSize())), ERRONEOUS.getValue())
                    .when(col(ITEMS_COUNT).between(ERRONEOUS.getOrderSize(), TINY.getOrderSize()), TINY.getValue())
                    .when(col(ITEMS_COUNT).between(TINY.getOrderSize(), MEDIUM.getOrderSize()), SMALL.getValue())
                    .when(col(ITEMS_COUNT).between(MEDIUM.getOrderSize(), LARGE.getOrderSize()), MEDIUM.getValue())
                    .otherwise(LARGE.getValue())
            );


        data = data.withColumn(MOST_POPULAR_ORDER_TYPE,
            max(struct(col(ITEMS_COUNT), col(ORDER_SIZE)))
                .over(Window
                    .partitionBy(RESTAURANT + FRANCHISE_ID, DATE))
                .getField(ORDER_SIZE));

        return data;
    }

    /**
     * <p>
     * This method applies additional logic to the input dataset data.
     * It adds a new column promo cold drinks which is set to true
     * if the average temperature is greater than 25.0, and false otherwise.
     * </p>
     * @param data The input dataset.
     * @return The dataset with the additional logic applied.
     */
    public Dataset<Row> applyAdditionalLogic(Dataset<Row> data) {
        return data.withColumn(
            PROMO_COLD_DRINKS,
            when(col(AVERAGE_TEMPERATURE).gt(25.0), lit(TRUE))
                .otherwise(lit(FALSE))
        );
    }
}
