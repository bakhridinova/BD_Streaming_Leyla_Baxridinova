package com.epam.uni.utils;

import org.junit.Test;

import static com.epam.uni.utils.Constants.AVERAGE_TEMPERATURE;
import static com.epam.uni.utils.Constants.CITY;
import static com.epam.uni.utils.Constants.COUNTRY;
import static com.epam.uni.utils.Constants.DATE;
import static com.epam.uni.utils.Constants.DISCOUNT;
import static com.epam.uni.utils.Constants.FRANCHISE_ID;
import static com.epam.uni.utils.Constants.FRANCHISE_NAME;
import static com.epam.uni.utils.Constants.ID;
import static com.epam.uni.utils.Constants.LATITUDE;
import static com.epam.uni.utils.Constants.LONGITUDE;
import static com.epam.uni.utils.Constants.RECEIPT;
import static com.epam.uni.utils.Constants.RESTAURANT;
import static com.epam.uni.utils.Constants.TOTAL_COST;
import static com.epam.uni.utils.Constants.WEATHER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DataReaderTest extends TestBase {
    @Test
    public void testReadReceiptsDataFor2022() {
        assertThat(receipts).isNotNull();
        assertThat(receipts.collectAsList().size())
            .isEqualTo(2);
        assertThat(receipts.schema().size())
            .isEqualTo(12);

        assertThat(receipts.schema().names())
            .contains(ID, RESTAURANT + FRANCHISE_ID)
            .contains(FRANCHISE_ID, FRANCHISE_NAME)
            .contains(RECEIPT + COUNTRY, RECEIPT + CITY)
            .contains(RECEIPT + LATITUDE, RECEIPT + LONGITUDE)
            .contains(RECEIPT + ID, RECEIPT + DATE)
            .contains(TOTAL_COST, DISCOUNT);
    }

    @Test
    public void testReadWeatherDataFor2022() {
        assertThat(weather).isNotNull();
        assertThat(weather.collectAsList().size())
            .isEqualTo(4);
        assertThat(weather.schema().size())
            .isEqualTo(6);

        assertThat(weather.schema().names())
            .contains(AVERAGE_TEMPERATURE, WEATHER + DATE)
            .contains(WEATHER + LATITUDE, WEATHER + LONGITUDE)
            .contains(WEATHER + COUNTRY, WEATHER + CITY);
    }
}

