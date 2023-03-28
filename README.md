
#  DiscountEngineCSV_to_Database

This is a Scala program that serves as a rule engine for qualifying orders' transactions to discounts based on a set of qualifying rules. It automatically calculates the proper discount based on the following calculation rules:


## Features
* The program is designed to monitor a specific directory for CSV files.
* The program processes only CSV files and skips others.
* The program reads the contents of the CSV file, calculates the discount and sends the data to the database.
* The program deletes the CSV file after processing it.
* The program creates a log file, which contains the processed data.
* The program calculates the execution time for processing each file.
## How to Use
You can start using the program by moving your files to Your_directory\\output\\rawData. The program will automatically process any CSV files that are placed in this directory.

The program uses a WatchService to monitor the directory for any new files. When a new file is detected, the program reads the contents of the CSV file, calculates the discount for each row of data, sends the data to the database, writes the data to a log file, and then deletes the file. The program will continue to monitor the directory for any new files until it is stopped.

To stop the program, simply call the stop() method on the CsvDirectoryListener object.
## Qualifying Rules
For example :
* If there are less than 30 days remaining for the product to expire (from the day of transaction, i.e. timestamp):

    * If 29 days remaining -> 1% discount
    * If 28 days remaining -> 2% discount
    * If 27 days remaining -> 3% discount 
    * Etc.

* Cheese and wine products are on sale:

    * Cheese -> 10% discount
    * Wine -> 5% discount
    * Products that are sold on 23rd of March have a special discount! (Celebrating the end of java project?)


* If the customer bought more than 5 of the same product:

    * 6 – 9 units -> 5% discount
    * 10‐14 units -> 7% discount
    * More than 15 -> 10% discount
## Code Description
The program is written in Scala and listens to a directory for incoming CSV files, which contain the transactions to be processed. Once a CSV file is received, the program reads its contents and applies the qualifying rules to calculate the proper discount for each transaction. The program then sends the processed data to a database and writes a log file of the processed transactions.

### CsvDirectoryListener
This class listens to a specified directory for incoming CSV files and processes them asynchronously. Once a CSV file is received, it reads its contents, applies the qualifying rules, and sends the processed data to the database. Finally, it deletes the CSV file and writes a log file of the processed transactions.

### calculateThePrice
This function calculates the price for each row of data in a CSV file. It takes in a list of strings representing a single row of data, as well as a discount function. It applies the discount function to the row of data to calculate the proper discount and then applies the discount to the price of the product to calculate the final price.

### Discount Functions
The discount functions are defined within the avg_Discount function. They apply the various qualifying rules to a given list of strings representing a single transaction and return the appropriate discount percentage based on the rules that apply.

### Rule1
This function applies rule 1, which is if the difference between the start date and end date is less than 30 days, apply a (30-remainedDays)% discount.

### Rule2
This function applies rule 2, which is if the transaction is for a cheese product, apply a 10% discount. If the transaction is for a wine product, apply a 5% discount.

### Rule3
This function applies rule 3, which is if the transaction was made on March 23rd, apply a 50% discount.

### Rule4
This function applies rule 4, which is if the customer bought more than 5 of the same product, apply a discount based on the number of units purchased.

### Rule5
This function applies rule 5, which is if the purchase was made through the app, apply a discount based on the quantity purchased.

### Rule6
This function applies rule 6, which is if the payment method is Visa, apply a 5% discount.
## Libraries
* java.io.{FileWriter, PrintWriter}
* java.text.SimpleDateFormat
* java.time.{LocalDate, LocalDateTime}
* java.time.format.DateTimeFormatter
* java.nio.file._
* scala.collection.JavaConverters._
* java.sql.DriverManager
* scala.concurrent.ExecutionContext.Implicits.global
* scala.concurrent.Future
## Installation

1. Clone this repository

2. Install the required dependencies

3. Compile the code using the following command:
```
scalac DiscountEngineCSV_to_Database.scala
```

## Usage
Start the program by running the following command:
```
scala Main
```
