package com.epam.uni;

import com.epam.uni.utils.DataProcessor;
import com.epam.uni.utils.DataReader;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static com.epam.uni.utils.Constants.HEADER_OPTION;
import static com.epam.uni.utils.Constants.PATH_TO_OUTPUT_DIRECTORY;
import static com.epam.uni.utils.Constants.PATH_TO_RECEIPT_RESTAURANTS_DATASET_DIRECTORY;
import static com.epam.uni.utils.Constants.PATH_TO_STREAMING_CHECKPOINT_DIRECTORY;
import static com.epam.uni.utils.Constants.PATH_TO_STREAMING_INPUT_DIRECTORY;
import static com.epam.uni.utils.Constants.PATH_TO_STREAMING_OUTPUT_DIRECTORY;
import static com.epam.uni.utils.Constants.PATH_TO_WEATHER_DATASET_DIRECTORY;
import static com.epam.uni.utils.Constants.SPARK_SESSION_APPLICATION_NAME;
import static com.epam.uni.utils.Constants.SPARK_SESSION_MASTER_URL;
import static com.epam.uni.utils.Constants.TRUE;

public class StreamingApplication {
    private final SparkSession session;
    private final DataReader dataReader;
    private final DataProcessor dataProcessor;

    public StreamingApplication() {
        session = SparkSession.builder()
            .appName(SPARK_SESSION_APPLICATION_NAME)
            .master(SPARK_SESSION_MASTER_URL)
            .getOrCreate();

        dataReader = new DataReader(session);
        dataProcessor = new DataProcessor();
    }

    public static void main(String[] args) {
        StreamingApplication application = new StreamingApplication();
//        application.prepareData();
        application.streamData();
    }

    private void prepareData() {
        // 3. Read the 2022 data for "receipt_restaurants" from the local storage,
        //    convert the "date_time" field into "date" format, and
        //    round latitude and longitude to two decimal digits.
        var receipts = dataReader
            .readReceiptsDataFor2022(PATH_TO_RECEIPT_RESTAURANTS_DATASET_DIRECTORY);

        // 4. Read the 2022 data for "weather" from the local storage,
        //    convert the "wthr_date" field into "date" format, and
        //    round latitude and longitude to two decimal digits.
        var weather = dataReader
            .readWeatherDataFor2022(PATH_TO_WEATHER_DATASET_DIRECTORY);

        // 5. Enrich "receipt_restaurants" with "weather" data (hint: use "join" on dates and coordinates).
        // 6. Filter the data by the average temperature ("avg_tmpr_c") of over 0 degrees Celsius.
        var joinData = dataProcessor
            .joinAndFilterData(receipts, weather);

        // 7. Calculate "real_total_cost" and "order_size" fields for each receipt
        //    ("total_cost" - "total_cost" * "discount"), use "restaurant_franchise_id"
        //    and "receipt_id" for grouping and this logic for "order_size"
        // 8. Calculate the quantity of each order type per restaurant per date and
        //    add "most_popular_order_type" for a restaurant for each day.
        var calculatedReceipts = dataProcessor
            .calculateFields(joinData);

        // Store the result in the local storage.
        calculatedReceipts.write()
            .mode(SaveMode.Overwrite)
            .option(HEADER_OPTION, TRUE)
            .csv(PATH_TO_OUTPUT_DIRECTORY);
    }

    private void streamData() {
        // 10. Prepare a set of CSV files (5-10 files) from the result of the previous point.
        var staticDataFrame = session.read()
            .option(HEADER_OPTION, TRUE)
            .csv(PATH_TO_OUTPUT_DIRECTORY)
            .schema();

        // 11. Using Spark Streaming read and process the data.
        var inputData = dataReader.readStream(
            staticDataFrame, PATH_TO_STREAMING_INPUT_DIRECTORY
        );

        // 12. Apply additional logic based on the average temperature ("avg_tmpr_c"),
        //     add "promo_cold_drinks" field as FALSE if "avg_tmpr_c" <= 25.0, and
        //     as TRUE if "avg_tmpr_c" > 25.0.
        var processedData = dataProcessor
            .applyAdditionalLogic(inputData);

        // 13. Store the processed data in the local storage.
        dataReader.writeStream(processedData,
            PATH_TO_STREAMING_OUTPUT_DIRECTORY,
            PATH_TO_STREAMING_CHECKPOINT_DIRECTORY
        );
    }
}
