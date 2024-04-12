package com.epam.uni.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
    // formats
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    // columns
    public static final String ID = "id";
    public static final String LONGITUDE = "lng";
    public static final String LATITUDE = "lat";
    public static final String COUNTRY = "country";
    public static final String CITY = "city";
    public static final String DATE = "date";
    public static final String DATE_TIME = "date_time";
    public static final String WEATHER_DATE = "wthr_date";
    public static final String FRANCHISE_ID = "franchise_id";
    public static final String FRANCHISE_NAME = "franchise_name";
    public static final String ORDER_SIZE = "order_size";
    public static final String ITEMS_COUNT = "items_count";
    public static final String DISCOUNT = "discount";
    public static final String TOTAL_COST = "total_cost";
    public static final String REAL_TOTAL_COST = "real_total_cost";
    public static final String AVERAGE_TEMPERATURE = "avg_tmpr_c";
    public static final String MOST_POPULAR_ORDER_TYPE = "most_popular_order_type";
    public static final String PROMO_COLD_DRINKS = "promo_cold_drinks";

    // paths
    private static final String BASE_PATH = "src/main/resources";
    private static final String STREAMING_FOLDER = "/streaming";
    public static final String PATH_TO_OUTPUT_DIRECTORY = BASE_PATH + "/result";
    public static final String PATH_TO_WEATHER_DATASET_DIRECTORY = BASE_PATH + "/weather";
    public static final String PATH_TO_RECEIPT_RESTAURANTS_DATASET_DIRECTORY = BASE_PATH + "/receipt_restaurants";
    public static final String PATH_TO_STREAMING_INPUT_DIRECTORY = PATH_TO_OUTPUT_DIRECTORY;
    public static final String PATH_TO_STREAMING_OUTPUT_DIRECTORY = PATH_TO_OUTPUT_DIRECTORY + STREAMING_FOLDER;
    public static final String PATH_TO_STREAMING_CHECKPOINT_DIRECTORY = PATH_TO_OUTPUT_DIRECTORY + STREAMING_FOLDER + "/checkpoint";

    // other
    public static final String CSV = "csv";
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String RECEIPT = "receipt_";
    public static final String WEATHER = "weather_";
    public static final String RESTAURANT = "restaurant_";
    public static final String OUTPUT_MODE = "append";
    public static final String HEADER_OPTION = "header";
    public static final String PATH_OPTION = "path";
    public static final String CHECKPOINT_LOCATION_OPTION = "checkpointLocation";
    public static String SPARK_SESSION_MASTER_URL = "local[*]";
    public static String SPARK_SESSION_APPLICATION_NAME = "spark practice";
}