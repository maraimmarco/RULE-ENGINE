# RULE-ENGINE
## **Overview**

This Scala project processes transaction data from a CSV file, calculates discounts based on predefined rules, logs the results to a file, and inserts the data into an Oracle database table.

## **Features**

- **Data Processing**: The program loads transaction data from a CSV file and processes it into product objects.
- **Discount Calculation**: It calculates discounts based on various rules defined in the **`DiscountRules`** object.
- **Logging**: The program logs product details and discount information to a specified log file.
- **Database Integration**: It inserts product details along with final discounted prices into an Oracle database table.
- **Customization**: Users can define additional discount rules or modify existing ones to suit their needs.

## **Usage**

1. **Data File**: Ensure that the CSV data file is located at the specified file path.
2. **Database Configuration**: Modify the database connection parameters (**`url`**, **`user`**, **`password`**) to connect to your Oracle database.
3. **Run**: Execute the **`scalaproject`** object to process the data, calculate discounts, log the results, and insert data into the database.

## **Files**

- **scalaproject.scala**: Main Scala file containing the project code.
- **TRX1000.csv**: Sample CSV file containing transaction records.
- **filelog.log**: Output log file where results are logged.
- **Oracle Database**: Ensure the Oracle database is running and accessible.

## **Dependencies**

- **Scala Standard Library**: The project uses standard Scala libraries for file I/O, date/time manipulation, and database connectivity.
- **Oracle JDBC Driver**: Ensure the Oracle JDBC driver is available in the classpath for database connectivity.

## **Customization**

- **Adding Discount Rules**: Additional discount rules can be defined by extending the **`DiscountRules`** object and updating the **`GetDiscountRules`** method.
- **Database Table**: Modify the database table structure and the corresponding SQL query in the **`insertDataIntoDatabase`** method to match your database schema.

## **Contributors**

- [MARIAM MARCOS]
