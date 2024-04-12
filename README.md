# Spark Streaming Application

This application is a Spark-based data processing pipeline that reads, processes, and writes data streams. It's designed to handle large volumes of data in real-time, making it ideal for applications that require immediate insights.

## Classes

The application consists of three main classes:

### 1. DataReader

The `DataReader` class is responsible for reading data from various sources. It has methods to read receipt data, weather data, and streaming data. Each method reads data from a given path, applies several transformations, and returns the formatted data.

- `readReceiptsData(String path)`: Reads receipt data for the year 2022. It adds a prefix to the date, lat, lng, country, and city columns, converts the date_time column to a date format, rounds the lat and lng columns to 2 decimal places, and filters the data to include only the records from the year 2022.

- `readWeatherData(String path)`: Reads weather data for the year 2022. It adds a prefix to the date, lat, lng, country, and city columns, converts the weather_date column to a date format, rounds the lat and lng columns to 2 decimal places, and filters the data to include only the records from the year 2022.

### 2. DataProcessor

The `DataProcessor` class is responsible for processing the data. It has methods to join and filter data, calculate fields, and apply additional logic.

- `joinAndFilterData(Dataset<Row> receipts, Dataset<Row> weather)`: Joins two datasets, `receipts` and `weather`, on multiple columns (date, lat, lng, country, city). After joining, it drops redundant columns from the `weather` dataset and renames the remaining columns. Finally, it filters out rows where the average_temperature is less than or equal to 0.

- `calculateFields(Dataset<Row> data)`: Calculates additional fields for the input dataset `data`. It first calculates the real_total_cost by subtracting the discount from the total_cost. Then, it groups the data by franchise_id and id, and aggregates various fields. It also calculates the order_size based on the items_count. Finally, it determines the most_popular_order_type for each franchise_id and date.

- `applyAdditionalLogic(Dataset<Row> data)`: Applies additional logic to the input dataset `data`. It adds a new column promo_cold_drinks which is set to `true` if the average_temperature is greater than 25.0, and `false` otherwise.

### 3. StreamingApplication

The `StreamingApplication` class is the main class that orchestrates the data processing pipeline. It initializes a SparkSession and instances of the DataReader and DataProcessor classes. It has two main methods: `prepareData` and `streamData`.

`prepareData()`: This method prepares the data for streaming. It performs the following steps:
 - Reads receipt and weather data for the year 2022 using the `DataReader`.
 - Joins the two datasets and filters the data based on the average temperature using the `DataProcessor`.
 - Calculates additional fields for each receipt and stores the result in the local storage.

`streamData()`: This method handles the streaming of data. It performs the following steps:
 - Reads a static DataFrame from the output directory.
 - Reads the streaming input data using the schema from the static DataFrame.
 - Applies additional logic to the input data using the `DataProcessor`, adding a new column `promo_cold_drinks` which is set to TRUE if the average temperature is greater than 25.0, and FALSE otherwise.
 - Writes the processed data to the streaming output directory using the `DataReader`, specifying a checkpoint location for the streaming query.

## Usage

To use this application, you need to have Apache Spark installed and configured on your machine. You can then run the `StreamingApplication` class, which will start the data processing pipeline.

Please note that the paths to the input and output directories, as well as the checkpoint location, are specified as constants in the `Constants` class and should be adjusted according to your environment.

## Conclusion

This Spark Streaming Application is a powerful tool for processing large volumes of data in real-time. It provides valuable insights and can be easily adapted to various use cases. Its modular design makes it easy to extend and customize according to your needs.
