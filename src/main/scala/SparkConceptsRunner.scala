import org.apache.spark.sql.SparkSession

object SparkConceptsRunner {
    def main(args: Array[String]) = {
        /*
          Spark Introduction
            # Package a jar containing your application
            $ sbt package

            # Use spark-submit to run your application
            YOUR_SPARK_HOME/bin/spark-submit --class "SimpleApp" --master local[4] target/scala-2.12/simple-project_2.12-1.0.jar
        */

        // Scala:
        //    An object in Scala is similar to a class, but defines a singleton instance that you can pass around.
        //    main takes an input parameter named args that must be typed as Array[String]

        val spark = SparkSession
                .builder
                .master("yarn")
                .appName("Spark Concepts Demo")
                .getOrCreate()

        println("#################################################################################")
        println("\n\n\n")

        println(spark)
        println("Spark Version : " + spark.version)

        println("\n\n\n")
        println("#################################################################################")
    }
}




