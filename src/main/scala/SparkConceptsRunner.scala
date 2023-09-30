import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object SparkConceptsRunner {
    def main(args: Array[String]) = {
        /*
          Spark Introduction
            # Using spark shell
            sudo su
            spark-shell --master yarn --name SparkTest

            # Use :paste for pasting scala code
            scala> :paste
            # ctrl-D to finish

            # Package a jar containing your application
            $ sbt package

            # Use spark-submit to run your application
            spark-submit --class SparkConceptsRunner --master yarn target/scala-2.12/sparkconcepts_2.12-0.1.0-SNAPSHOT.jar
        */

        println("#################################################################################")
        println("\n\n\n")

        /*
            SparkSession:
                - SparkSession was introduced in version Spark 2.0, It is an entry point to underlying Spark functionality
                    in order to programmatically create Spark RDD, DataFrame, and DataSet.
         */

        val spark = SparkSession
                .builder
                .master("yarn")
                .appName("Spark Concepts Demo")
                .getOrCreate()

        println("Spark object: " + spark)
        println("Spark Version : " + spark.version)

        // get all spark configs
        val configMap:Map[String, String] = spark.conf.getAll
        print("Spark configs: " + configMap)

        /*
             Resilient Distributed Datasets (RDD)
                - It is an immutable distributed collection of objects. It is a fundamental data structure of Spark,
                - Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster.

            DataFrame
                - It is a distributed collection of data organized into named columns. It is conceptually equivalent to a
                table in a relational database or a data frame in R/Python, but with richer optimizations under the hood.
                - DataFrames can be constructed from a wide array of sources such as: structured data files, tables
                in Hive, external databases, or existing RDDs.
         */

        // Create a dataframe from csv
        println("Creating dataframe from movies.csv")
        val dataFrame = spark.read.options(
            Map ("header" -> "true",
                "inferSchema" -> "true",
                "mode" -> "failfast")
        ).csv("/sparkdemo/movies.csv")

        /*
            Generic format:
            val dataFrame = spark.read
                                .format("csv")
                                .option("header", "true")
                                .option("inferSchema", "true")
                                .option("mode", "failfast")
                                .option("path", "/sparkdemo/movies.csv")
                                .load()
         */

        println("Total partitions of dataframe: " + dataFrame.rdd.getNumPartitions);

        // Create a dataframe with 5 partitions
        // val dataFrame5 = dataFrame.repartition(5).toDF
        // println("Total partitions in new dataframe: " + dataFrame5.rdd.getNumPartitions);

        println("Dataframe Schema for movies.csv: ")
        dataFrame.printSchema

        println("Dataframe Contents: ")
        // by default this shows 20 rows
        dataFrame.select("index", "movie_name", "genre", "category", "imdb_rating").show(false)
        // show 5 rows
        println("")
        dataFrame.select("index", "movie_name", "genre", "category").show(5, false)

        // Create another dataframe with above 4 columns
        val newDataFrame = dataFrame.select("index", "movie_name", "genre", "category")

        // Filtering with where()
        println("Filtering dataframe:")
        dataFrame
                .select("index", "movie_name", "genre", "category", "imdb_rating")
                .where("category == 'R'")
                .show(dataFrame.count.toInt, false)

        // Create a temporary view from dataframe. This is similar to sql views
        println("creating temp view movie_tbl from dataframe")
        dataFrame.createOrReplaceTempView("movie_tbl")

        // Show spark catalog.
        // A catalog is an interface, that allows you to create, drop, alter, or query underlying databases, tables and functions.
        println("Spark catalog:")
        spark.catalog.listTables.show

        // We can execute sql queries on this view and get a dataframe
        val sqlOutputDf = spark.sql(
            """
              | select index, movie_name, genre, category, imdb_rating
              | from movie_tbl
              | where imdb_rating > 8 and category == 'R'
              |""".stripMargin)

        println("Executing sql query on movie_tbl")
        sqlOutputDf.show(sqlOutputDf.count.toInt, false)

        // The sql works as fast as dataframe transformations.
        // https://stackoverflow.com/questions/66014494/are-built-in-spark-transformations-faster-than-spark-sql-queries#:~:text=1%20Answer&text=As%20per%20the%20documentation%20the,default%20abstraction%20that%20spark%20provides.
        // https://www.confessionsofadataguy.com/dataframes-vs-sparksql-which-one-should-you-choose/

        /*
            User Defined Functions (UDF)
                - In Spark, you create UDF by creating a function in a language you prefer to use for Spark.
                - For example, if you are using Spark with scala, you create a UDF in scala language and wrap it with
                    udf() function or register it as udf to use it on DataFrame and SQL respectively.
                - UDF’s are used to extend the functions of the framework and re-use this function on several DataFrame.
                - For example if you wanted to convert the every first letter of a word in a sentence to capital case,
                    spark build-in features does’t have this function hence you can create it as UDF and reuse this as
                    needed on many Data Frames.
                -  Before you create any UDF, do your research to check if the similar function you wanted is already
                    available in Spark SQL Functions.
                - When you creating UDF’s you need to design them very carefully otherwise you will come across
                    performance issues.
         */

        val convertToUpperCaseUdf = udf(StringUtils.convertToUpperCase)

        println("Udf function convertToUpperCase demo: ")
        dataFrame.select(
                    col("index"),
                    col("movie_name"),
                    convertToUpperCaseUdf(col("genre")).as("genre_uppercase"),
                    col("category"),
                    col("imdb_rating"))
                .show(false)

        // Registering Spark UDF to use it on SQL
        spark.udf.register("convertToUpperCase", StringUtils.convertToUpperCase)

        val sqlOutputDf2 = spark.sql(
            """
              | select index, movie_name, convertToUpperCase(genre) as genre_uppercase, category, imdb_rating
              | from movie_tbl
              | where imdb_rating > 8 and category == 'R'
              |""".stripMargin)

        println("Udf function in spark sql: ")
        sqlOutputDf2.show(sqlOutputDf2.count().toInt, false)


        /*
            Spark JDBC
                # Spark Shell
                spark-shell --master yarn --name SparkTest --jars /spark/deployment/dependencies/mysql-connector-j-8.0.31.jar

                # Spark Submit
                spark-submit --jars /spark/deployment/dependencies/mysql-connector-j-8.0.31.jar

                jdbc:mysql://app1.stguat.unicommerce.infra:3306/uniware


         */

        val addressDetailsFetchQuery =
            """
              |select
              |     name,
              |     turbo_mobile,
              |     address_line1,
              |     address_line2,
              |     city,
              |     district,
              |     state_code,
              |     country_code,
              |     pincode
              |from shipping_package_address
              |limit 10
              |""".stripMargin

        val jdbcOptions = Map(
            "driver" -> "com.mysql.cj.jdbc.Driver",
            "url" -> "jdbc:mysql://db.stgaddress-master.unicommerce.infra:3306/turbo",
            "user" -> "root",
            "password" -> "uniware"
        )

        println("Fetching addresses from jdbc:")
        val jdbcAddressDF = spark.read
                .format("jdbc")
                .options(jdbcOptions)
                .option("query", addressDetailsFetchQuery)
                .option("header", "true")
                .option("inferSchema", "true")
                .option("mode", "failfast")
                .load()

        println("Addresses contents:")
        jdbcAddressDF.show(false)

        println("Modifying state code of address to uppercase")
        val newJdbcAddressDF = jdbcAddressDF.withColumn("state_code", convertToUpperCaseUdf(col("state_code")))

        println("New Addresses:")
        newJdbcAddressDF.show(false)

        println("Writing new addresses to jdbc")
        newJdbcAddressDF.write
                .format("jdbc")
                .options(jdbcOptions)
                .option("dbtable", "new_test")
                .save()

        println("Done")
        println("\n\n\n")
        println("#################################################################################")
    }
}




