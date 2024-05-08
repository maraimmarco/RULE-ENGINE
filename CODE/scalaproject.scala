import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import scala.io.Source
import java.sql.{Connection, DriverManager}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Date
object scalaproject extends App {

    // Initialize FileWriter for logging
    val logFilePath =  "C:\\Users\\Resal\\Downloads\\filelog.log"
    val logFile = new File(logFilePath)
    val fileWriter = new FileWriter(logFile)
    //to get current timestamp
    def getCurrentTimestamp: String = {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val currentTime = new Date()
        dateFormat.format(currentTime)
    }
    // Function to log messages with timestamp and write to file
    def log(message: String): Unit = {
        val timestamp = getCurrentTimestamp
        val logMessage = s"$timestamp INFO $message"
        println(logMessage) // Print log message to console
        fileWriter.write(logMessage + "\n") // Write log message to file
    }
    // Load data
    val filePath = "src/main/scala/TRX1000.csv"
    val lines: List[String] = Source.fromFile(filePath).getLines().drop(1).toList // drop header

    // Case class to represent a product
    case class Product(timestamp: String, productName: String, expiryDate: String, quantity: Int, unitPrice: Double, channel: String, paymentMethod: String)

    // Function to parse a line into a Product object
    def product(line: String): Product = {
        val splitFields = line.split(",")
        Product(splitFields(0), splitFields(1), splitFields(2), splitFields(3).toInt, splitFields(4).toDouble, splitFields(5), splitFields(6))
    }

    // Function to calculate the remaining days
    def remainingDays(product: Product): Int = {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val timestamp = dateFormat.parse(product.timestamp)
        val expiryDate = dateFormat.parse(product.expiryDate)
        ((expiryDate.getTime - timestamp.getTime) / (1000 * 60 * 60 * 24)).toInt
    }

    case class DiscountRule(QualifyingCondition: Product => Boolean, GetDiscount: Product => Double)

    // Define the DiscountRules object
    object DiscountRules {
        // Qualifying condition for discount A
        def isAQualified(product: Product): Boolean = {
            val daysDifference = remainingDays(product)
            daysDifference < 30
        }

        // Qualifying condition for discount B
        def isBQualified(product: Product): Boolean = {
            val productName = product.productName.toLowerCase
            productName.contains("cheese") || productName.contains("wine")
        }

        def isCQualified(product: Product): Boolean = {
            val daysInMarch = product.timestamp.substring(0, 10)
            daysInMarch == "2023-03-23"
        }

        def isDQualified(product: Product): Boolean = {
            val numberOfProducts = product.quantity
            if (numberOfProducts < 6) {
                return false
            } else {
                return true
            }
        }

        def isEQualified(product: Product): Boolean = {
            val channelType = product.channel
            channelType == "APP"
        }
        def isFQualified(product: Product):Boolean={
            val paymentMethod=product.paymentMethod
            paymentMethod=="Visa"
        }
        def F(product: Product):Double ={
            if (isFQualified(product)){
                return 0.05
            }else{
                0.0
            }
        }
        def E(product: Product): Double = {
            if(isEQualified(product)){
                val discountPercentages = List(
                    (5, 5),  // Quantity: 1-5, Discount: 5%
                    (6, 10), // Quantity: 6-10, Discount: 10%
                    (11, 15), // Quantity: 11-15, Discount: 15%
                    (16,20)
                )

                val quantity = product.quantity
                val roundedQuantity = math.ceil(quantity.toDouble / 5) * 5 // Round up to nearest multiple of 5
                val (minQty, discount) = discountPercentages.find { case (min, _) => roundedQuantity <= min }.getOrElse((Int.MaxValue, 0))
                discount / 100.0
            }else{
                0.0
            }
        }

        // Function to calculate discount A
        def A(product: Product): Double = {
            val remaining = remainingDays(product)
            if (!isAQualified(product)) {
                // Product doesn't qualify for discount
                return 0.0
            }
            // Calculate discount based on remaining days
            val discountPercentage = 1.0 + (29 - remaining)
            discountPercentage / 100.0
        }

        // Function to calculate discount B
        def B(product: Product): Double = {
            val productName = product.productName.toLowerCase
            if (isBQualified(product)) {
                if (productName.contains("cheese")) {
                    return 0.1 // 10% discount
                } else if (productName.contains("wine")) {
                    return 0.05 // 5% discount
                }
            }
            0.0 // No discount or product doesn't qualify
        }

        def C(product: Product): Double = {
            if (isCQualified(product)) {
                return 0.5 // 50% discount
            }
            0.0 // No discount or product doesn't qualify
        }

        def D(product: Product): Double = {
            val numberOfProducts = product.quantity
            if (isDQualified(product)) {
                if (numberOfProducts >= 6 && numberOfProducts <= 9) {
                    return 5.0 / 100 //discount
                } else if (numberOfProducts >= 10 && numberOfProducts <= 14) {
                    return 7.0 / 100
                } else if (numberOfProducts <= 15) {
                    return 10.0 / 100
                }
            }
            0.0
        }

        def GetDiscountRules(): List[DiscountRule] = {
            List(
                DiscountRule(isAQualified, A),
                DiscountRule(isBQualified, B),
                DiscountRule(isCQualified, C),
                DiscountRule(isDQualified, D),
                DiscountRule(isEQualified, E),
                DiscountRule(isFQualified,F)
            )
        }
    }

    // Parse the lines into Product objects
    val products: List[Product] = lines.map(product)
    val discountsPerProduct: List[List[Double]] = products.map { product =>
        List(
            DiscountRules.A(product),
            DiscountRules.B(product),
            DiscountRules.C(product),
            DiscountRules.D(product),
            DiscountRules.E(product),
            DiscountRules.F(product)
        )
    }
    def calculateFinalPrice(product: Product, discounts: List[Double]): Double = {
        val totalDiscount = discounts.sum
        val discountedPrice = product.unitPrice * product.quantity*(1 - totalDiscount)
        discountedPrice
    }
    val topTwoDiscountsAndAverage: List[Double] = discountsPerProduct.map { discounts =>
        val sortedDiscounts = discounts.sorted(Ordering[Double].reverse)
        val topTwoDiscounts = sortedDiscounts.take(2)
        topTwoDiscounts.sum / topTwoDiscounts.length
    }

    // Log top two discounts and their average
    products.zipWithIndex.foreach { case (product, index) =>
        val logMessage = s"Product ${index + 1}: Name - ${product.productName}, Average of Top Two Discounts - ${topTwoDiscountsAndAverage(index)}"
        println(logMessage) // Print log message to console
        fileWriter.write(logMessage + "\n") // Write log message to file
    }

    // Database connection parameters
    val url = "jdbc:oracle:thin:@//localhost:1521/XE"
    val user = "HR"
    val password = "root"
    // Define a function to insert data into the database table
    def insertDataIntoDatabase(product: Product, finalPrice: Double): Unit = {
        val connection: Connection = DriverManager.getConnection(url, user, password)
        val query = "INSERT INTO SCALA_TABLE (productName, finalPrice) VALUES (?, ?)"
        val statement: PreparedStatement = connection.prepareStatement(query)
        statement.setString(1, product.productName)
        statement.setDouble(2, finalPrice)
        statement.executeUpdate()
        statement.close()
        connection.close()
    }

    // Calculate final price and insert into the database
    products.zip(discountsPerProduct).foreach { case (product, discounts) =>
        val finalPrice = calculateFinalPrice(product, discounts)
        insertDataIntoDatabase(product, finalPrice)
    }

    // Close FileWriter after logging
    fileWriter.close()
}
