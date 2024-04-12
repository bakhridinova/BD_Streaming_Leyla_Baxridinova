package com.epam.uni.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import static com.epam.uni.utils.TestConstants.PATH_TO_TEST_RECEIPT_RESTAURANTS_DATASET_DIRECTORY;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DataReaderTest extends TestBase {
    @Test
    public void testReadReceiptsData() {
        assertThat(receipts).isNotNull();
        assertThat(receipts.collectAsList().size())
            .isEqualTo(6);
    }

    @Test
    public void testReadWeatherData() {
        assertThat(weather).isNotNull();
        assertThat(weather.collectAsList().size())
            .isEqualTo(6);
    }

    @Test
    public void testReadStream() {
        StructType schema = receipts.schema();
        Dataset<Row> stream = dataReader
            .readStream(schema, PATH_TO_TEST_RECEIPT_RESTAURANTS_DATASET_DIRECTORY);

        assertThat(stream).isNotNull();
        assertThat(schema.length())
            .isEqualTo(13);
        assertThat(schema.names())
            .contains("id")
            .contains("franchise_id")
            .contains("franchise_name")
            .contains("restaurant_franchise_id")
            .contains("country")
            .contains("city")
            .contains("lat")
            .contains("lng")
            .contains("receipt_id")
            .contains("total_cost")
            .contains("discount")
            .contains("date_time")
            .contains("date");
    }
}

