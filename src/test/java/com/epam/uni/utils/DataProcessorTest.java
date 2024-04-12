package com.epam.uni.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DataProcessorTest extends TestBase {
    @Test
    public void testEnrichAndFilterData() {
        Dataset<Row> result = dataProcessor
            .enrichAndFilterData(receipts, weather);

        assertThat(result).isNotNull();
        assertThat(result.collectAsList().size())
            .isEqualTo(1);
        Row firstRow = result.first();
        assertThat(firstRow.getString(0))
            .isEqualTo("188978561075");
        assertThat(firstRow.getString(1))
            .isEqualTo("52");
        assertThat(firstRow.getString(2))
            .isEqualTo("The Red Door");
        assertThat(firstRow.getString(3))
            .isEqualTo("5034");
        assertThat(firstRow.getString(4))
            .isEqualTo("56df62bf-f7e7-47ff-8800-475bf46262cf");
        assertThat(firstRow.getString(5))
            .isEqualTo("17.40");
        assertThat(firstRow.getString(6))
            .isEqualTo("0.15");
        assertThat(firstRow.getString(7))
            .isEqualTo("2022-08-14T17:32:50.000Z");
    }

    @Test
    public void testCalculateFields() {
        Dataset<Row> result = dataProcessor
            .calculateFields(receipts);

        assertThat(result).isNotNull();
        assertThat(result.collectAsList().size())
            .isEqualTo(6);
    }
}
