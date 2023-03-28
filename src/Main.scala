

import scala.collection.immutable.List
import java.io.{FileWriter, PrintWriter}
import java.lang.Integer
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.nio.file._
import scala.collection.JavaConverters._
import java.sql.DriverManager
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object Main extends App {
  println("Hi , this program is designed to listen to the files and processed it and convert it into database ")
  println("Please provide first the directory you want the app to listen  ")
  val path =scala.io.StdIn.readLine()

  //val listener = new CsvDirectoryListener("C:\\Users\\20101\\Desktop\\output\\rawData")
  val listener = new CsvDirectoryListener(path)

  listener.start()

  class CsvDirectoryListener(directoryPath: String) {

    // Create a Path object for the directory
    private val dir = Paths.get(directoryPath)

    // Create a WatchService to monitor the directory
    private val watcher = FileSystems.getDefault.newWatchService()

    // Register the directory with the WatchService for ENTRY_CREATE events
    private val key = dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE)

    // Print a message indicating where to move files to start the program
    println(s"You can start use the program by moving your files to $directoryPath ")

    // Method to start the listener
    def start(): Unit = {

      // Continuously poll the WatchService for events
      while (true) {
        val events = key.pollEvents().asScala

        // Iterate over the events and process them asynchronously
        for (event <- events) {
          Future {

            // Check the type of event
            event.kind match {

              // If a file was created...
              case StandardWatchEventKinds.ENTRY_CREATE =>

                // Get the filename and check if it's a CSV file
                val filename = event.context.asInstanceOf[Path].toString
                if (filename.endsWith(".csv")) {

                  // Create a Path object for the file
                  val filePath = dir.resolve(filename)
                  println(s"$filename is received")

                  // Read the contents of the CSV file
                  val bufferedSource = io.Source.fromFile(filePath.toString)
                  val list = bufferedSource.getLines().toList
                  val data = list.map(_.split(",").toList).drop(1)

                  // Calculate the price for each row of data
                  val x = data.map(calculateThePrice(_, avg_Discount))

                  // Send the data to the database and write to the log file
                  val startTime = System.nanoTime()
                  listToDatabase(x)
                  listToLog(x, "C:\\Users\\20101\\Desktop\\output\\logs.log", filename)
                  println(s"$filename is processed and sent to the database")
                  bufferedSource.close()

                  // Delete the file
                  Files.delete(filePath)

                  // Print the execution time
                  val endTime = System.nanoTime()
                  val executionTime = (endTime - startTime) / 1000000.0 / 1000 // Convert to milliseconds
                  println(s"Execution time: $executionTime s")

                }

              case _ =>
            }

            // Reset the WatchKey to receive more events
            key.reset()
          }
        }
      }
    }

    // Method to stop the listener
    def stop(): Unit = {
      watcher.close()
    }
  }


  // This function calculates the discount based on a set of rules for a given list of strings
  def avg_Discount(discountList: List[String]): Float = {

    // Rule 1: If the difference between the start date and end date is less than 30 days, apply a (30-days_difference)% discount
    def rule1(startDate: String, endDate: String): Int = {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val format2 = new SimpleDateFormat("yyyy-MM-dd")

      val date_1 = format.parse(startDate)
      val date_2 = format2.parse(endDate)
      val time_difference = date_2.getTime - date_1.getTime
      val days_difference = (time_difference / (1000 * 60 * 60 * 24)) % 365

      if (days_difference < 30) {
        val x = 30 - days_difference.toInt
         x
      } else {
        0
      }
    }

    // Rule 6: If the payment method is Visa, apply a 5% discount
    def rule6(inputString: String): Int = {
      if (inputString.toLowerCase.trim == "visa") {
         5
      } else {
         0
      }
    }

    // Rule 5: If the purchase was made through the app, apply a discount based on the quantity purchased
    def rule5(quantity: String, through: String): Int = {
      try {
        if (through.toLowerCase.trim == "app") {
          // If the quantity is a multiple of 5, apply no discount
          // Otherwise, round up the quantity to the nearest multiple of 5 and subtract the original quantity
          return (math.ceil(quantity.toDouble / 5) * 5.0).toInt
        } else {
          return 0
        }

      } catch {
        // If the quantity cannot be parsed to a number, apply no discount
        case _: NumberFormatException => return 0
      }
    }

    // Rule 3: If the purchase was made on March 23, 2023, apply a 50% discount
    def rule3(dateString: String): Int = {
      val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
      val date = LocalDate.parse(dateString.trim, formatter)

      if (date.getYear == 2023 && date.getMonthValue == 3 && date.getDayOfMonth == 23) {
        return 50
      } else {
        return 0
      }
    }

    // Rule 4: Apply a discount based on the number of units purchased
    def rule4(unitsString: String): Int = {
      if (unitsString.toIntOption.isDefined) {
        val numunits = unitsString.toInt
        if (numunits >= 6 && numunits <= 9) {
          5
        } else if (numunits >= 10 && numunits <= 14) {
          7
        } else if (numunits >= 15) {
          10
        } else {
          0
        }
      } else {
        0
      }
    }

    // Rule 2: Apply a discount based on the name of the product
    def rule2(productName: String): Int = {
      val cheeseDiscount = 10
      val wineDiscount = 5

      productName.toLowerCase match {
        case name if name.contains("cheese") => cheeseDiscount
        case name if name.contains("wine") => wineDiscount
        case _ => 0
      }
    }

    val col0 = discountList(0)
    val col1 = discountList(1)
    val col2 = discountList(2)
    val col3 = discountList(3)
    val col4 = discountList(4)
    val col5 = discountList(5)
    val col6 = discountList(6)
    // put all the discounts into one list to calculate the largest two numbers
    val listOfDiscounts = List(rule1(col0, col2), rule2(col1), rule3(col0), rule4(col3), rule5(col3, col5), rule6(col6))
    val list = listOfDiscounts.sorted.takeRight(2)
    list(0) match {
      case 0 => (list(1)) / 1.0.toFloat
      case _ => (list(0) + list(1)) / 2.0.toFloat
    }

  }

  // This function takes a list of strings containing data of a single row from a CSV file and the function to calculate average discount
  // It returns a list of strings with the calculated end discount percentage and final price for the row
  def calculateThePrice(data: List[String], avg_Discount: List[String] => Float): List[String] = {

    // Calculate the end discount percentage using the provided function
    val endDiscount = avg_Discount(data)

    // Add the end discount percentage to the input data list
    val dataWithoutFinalPrice = data :+ (endDiscount.toString + "%")

    // Calculate the final price using the price and quantity columns and the end discount percentage
    val finalPrice = data(3).toFloat * data(4).toFloat * (100 - endDiscount) / 100

    // Add the final price to the list
    val dataWithFinalPrice = dataWithoutFinalPrice :+ (finalPrice).toString

    // Return the updated list
    return dataWithFinalPrice
  }

  // This function writes a log message to a file with the current date and time, the name of the processed CSV file, and the number of rows inserted to the database
  def listToLog(list: List[List[String]], pathWriter: String, fileName: String) = {
    // Open the log file in append mode
    val fileWriter = new FileWriter(pathWriter, true)

    // Create a PrintWriter to write to the file
    val printWriter = new PrintWriter(fileWriter)

    // Get the number of rows in the input list
    val noOfRows = list.size

    // Write the log message to the file
    printWriter.println(s"${LocalDateTime.now()},$fileName is processed and inserted to the database $noOfRows rows ")

    // Close the PrintWriter and FileWriter
    printWriter.close()
    fileWriter.close()
  }

  // This function inserts the input list of rows to a MySQL database
  def listToDatabase(list: List[List[String]]) = {
    // Open a connection to the database using the provided JDBC URL, username, and password
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/iwish", "root", "root")

    // Prepare a statement to insert a row into the "storedata" table with placeholders for the column values
    val pst = conn.prepareStatement("insert into storedata values (STR_TO_DATE(?, '%Y-%m-%dT%H:%i:%sZ'),?,?,?,?,?,?,?,?) ")

    try {
      // Set the batch size to 1000
      val batchSize = 1000

      // Initialize the row count to 0
      var count = 0

      // Loop through the input list of rows
      for (row <- list) {
        // Set the values for the columns in the prepared statement using the current row
        pst.setString(1, row(0))
        pst.setString(2, row(1))
        pst.setString(3, row(2))
        pst.setString(4, row(3))
        pst.setString(5, row(4))
        pst.setString(6, row(5))
        pst.setString(7, row(6))
        pst.setString(8, row(7))
        pst.setString(9, row(8))

        // Add the prepared statement to the batch
        pst.addBatch()

        // Increment the row count
        count += 1

        // If the batch size is reached, execute the batch and clear it
        if (count % batchSize == 0) {
          pst.executeBatch()
          pst.clearBatch()
        }
      }

      // Execute the final batch update
      pst.executeBatch()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      conn.close()
    }
  }

}




