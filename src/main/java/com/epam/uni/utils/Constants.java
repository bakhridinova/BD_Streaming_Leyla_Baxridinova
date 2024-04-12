package com.epam.uni.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
    // formats
    public static String DATE_FORMAT = "yyyy-MM-dd";
    public static String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    // columns
    public static String LONGITUDE_COLUMN = "lng";
    public static String LATITUDE_COLUMN = "lat";
    public static String COUNTRY_COLUMN = "country";
    public static String CITY_COLUMN = "city";
    public static String DATE_COLUMN = "date";
    public static String DATE_TIME_COLUMN = "date_time";
    public static String WEATHER_DATE_COLUMN = "wthr_date";

    // other
    public static String CSV = "csv";
    public static String SPARK_SESSION_MASTER_URL = "local[*]";
    public static String SPARK_SESSION_APPLICATION_NAME = "spark practice";
    public static String PATH_TO_OUTPUT_DIRECTORY = "src/main/resources/result";
    public static String PATH_TO_CHECKPOINT_LOCATION = "src/main/resources/result/checkpoint";
    public static String PATH_TO_RECEIPT_RESTAURANTS_DATASET_DIRECTORY = "src/main/resources/receipt_restaurants";
    public static String PATH_TO_WEATHER_DATASET_DIRECTORY = "src/main/resources/weather";
}