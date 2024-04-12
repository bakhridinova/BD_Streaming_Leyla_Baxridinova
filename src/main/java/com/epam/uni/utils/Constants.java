package com.epam.uni.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
    // formats
    public String DATE_FORMAT = "yyyy-MM-dd";
    public String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    // columns
    public String LONGITUDE_COLUMN = "lng";
    public String LATITUDE_COLUMN = "lat";
    public String COUNTRY_COLUMN = "country";
    public String CITY_COLUMN = "city";
    public String DATE_COLUMN = "date";
    public String DATE_TIME_COLUMN = "date_time";
    public String WEATHER_DATE_COLUMN = "wthr_date";

    // other
    public String CSV = "csv";
    public String PATH_TO_OUTPUT_DIRECTORY = "src/main/resources/result";
    public String PATH_TO_CHECKPOINT_LOCATION = "src/main/resources/result/checkpoint";
    public String PATH_TO_RECEIPT_RESTAURANTS_DATASET_DIRECTORY = "src/main/resources/receipt_restaurants";
    public String PATH_TO_WEATHER_DATASET_DIRECTORY = "src/main/resources/weather";
}