package com.epam.uni.utils;

import org.apache.spark.sql.Row;
import org.junit.Test;

import java.sql.Date;
import java.util.List;

import static com.epam.uni.utils.Constants.AVERAGE_TEMPERATURE;
import static com.epam.uni.utils.Constants.CITY;
import static com.epam.uni.utils.Constants.COUNTRY;
import static com.epam.uni.utils.Constants.DATE;
import static com.epam.uni.utils.Constants.DISCOUNT;
import static com.epam.uni.utils.Constants.FRANCHISE_ID;
import static com.epam.uni.utils.Constants.FRANCHISE_NAME;
import static com.epam.uni.utils.Constants.ID;
import static com.epam.uni.utils.Constants.ITEMS_COUNT;
import static com.epam.uni.utils.Constants.LATITUDE;
import static com.epam.uni.utils.Constants.LONGITUDE;
import static com.epam.uni.utils.Constants.MOST_POPULAR_ORDER_TYPE;
import static com.epam.uni.utils.Constants.ORDER_SIZE;
import static com.epam.uni.utils.Constants.REAL_TOTAL_COST;
import static com.epam.uni.utils.Constants.RECEIPT;
import static com.epam.uni.utils.Constants.RESTAURANT;
import static com.epam.uni.utils.Constants.TOTAL_COST;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DataProcessorTest extends TestBase {
    @Test
    public void testJoinAndFilterData() {
        joinData = dataProcessor.joinAndFilterData(receipts, weather);

        assertThat(joinData).isNotNull();
        List<Row> rows = joinData.collectAsList();
        assertThat(rows.size()).isEqualTo(2);

        Row firstRow = rows.get(0);
        assertThat(firstRow.getString(0)).isEqualTo("188978561075");
        assertThat(firstRow.getString(1)).isEqualTo("52");
        assertThat(firstRow.getString(2)).isEqualTo("The Red Door");
        assertThat(firstRow.getString(3)).isEqualTo("5034");
        assertThat(firstRow.getString(4)).isEqualTo("AT");
        assertThat(firstRow.getString(5)).isEqualTo("Vienna");
        assertThat(firstRow.getDouble(6)).isEqualTo(48.85);
        assertThat(firstRow.getDouble(7)).isEqualTo(2.39);
        assertThat(firstRow.getString(8)).isEqualTo("56df62bf-f7e7-47ff-8800-475bf46262cf");
        assertThat(firstRow.getString(9)).isEqualTo("17.40");
        assertThat(firstRow.getString(10)).isEqualTo("0.15");
        assertThat(firstRow.getDate(11)).isEqualTo(Date.valueOf("2022-08-14"));
        assertThat(firstRow.getString(12)).isEqualTo("19.02");

        Row secondRow = rows.get(1);
        System.out.println(secondRow);
        assertThat(secondRow.getString(0)).isEqualTo("266287972391");
        assertThat(secondRow.getString(1)).isEqualTo("40");
        assertThat(secondRow.getString(2)).isEqualTo("Crimson Cafe");
        assertThat(secondRow.getString(3)).isEqualTo("70700");
        assertThat(secondRow.getString(4)).isEqualTo("RU");
        assertThat(secondRow.getString(5)).isEqualTo("Moscow");
        assertThat(secondRow.getDouble(6)).isEqualTo(45.47);
        assertThat(secondRow.getDouble(7)).isEqualTo(9.19);
        assertThat(secondRow.getString(8)).isEqualTo("4cbfe14a-77ab-489e-aeb8-192931ad493a");
        assertThat(secondRow.getString(9)).isEqualTo("13.90");
        assertThat(secondRow.getString(10)).isEqualTo("0.0");
        assertThat(secondRow.getDate(11)).isEqualTo(Date.valueOf("2022-08-16"));
        assertThat(secondRow.getString(12)).isEqualTo("25.37");
    }

    @Test
    public void testCalculateFields() {
        var result = dataProcessor.calculateFields(joinData);

        assertThat(result).isNotNull();
        assertThat(result.schema().names())
            .contains(ID, RESTAURANT + FRANCHISE_ID)
            .contains(FRANCHISE_ID, FRANCHISE_NAME)
            .contains(COUNTRY, CITY)
            .contains(LATITUDE, LONGITUDE)
            .contains(RECEIPT + ID, DATE)
            .contains(REAL_TOTAL_COST, TOTAL_COST, DISCOUNT)
            .contains(AVERAGE_TEMPERATURE, MOST_POPULAR_ORDER_TYPE)
            .contains(ITEMS_COUNT, ORDER_SIZE);
    }

    @Test
    public void testApplyAdditionalLogic() {
        var result = dataProcessor
            .applyAdditionalLogic(calculatedData);

        assertThat(result).isNotNull();
        assertThat(result.schema().names())
            .contains(AVERAGE_TEMPERATURE);

        List<Row> rows = result.collectAsList();
        assertThat(rows.size()).isEqualTo(2);
        // average temperature less than 25.0
        assertThat(rows.get(0).getString(17))
            .isEqualTo("false");
        // average temperature greater than 25.0
        assertThat(rows.get(1).getString(17))
            .isEqualTo("true");
    }
}
