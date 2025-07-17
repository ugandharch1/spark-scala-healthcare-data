package Adherence

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AdherenceETL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Medication Adherence ETL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputPath = "C:\\Users\\Ugandharch\\IdeaProjects\\MedicationAdherenceETL\\src\\main\\resources\\Diabetes_Adherence_Data.csv"
    val rawDF = spark.read.option("header", "true").csv(inputPath)

    println("🔍 Raw Data:")
    rawDF.show(5)

    // 1. Clean the data
    val cleanedDF = cleanData(rawDF)(spark)
    println("✅ Cleaned Data:")
    cleanedDF.show(5)

    // 2. Compute PDC
    val pdcDF = computePDC(cleanedDF)(spark)
    println("📈 PDC Calculation:")
    pdcDF.show(false)

    // 3. Segment Patients by Adherence
    val segmentedDF = adherenceSegment(cleanedDF)(spark)
    println("🧮 Adherence Segments:")
    segmentedDF.select("MEMBER", "adherence_score", "adherence_segment").show(10, truncate = false)

    // 4. Monthly Adherence Trend
    val trendDF = monthlyAdherenceTrend(cleanedDF)(spark)
    println("📅 Monthly Adherence Trend:")
    trendDF.show(10, truncate = false)

    spark.stop()
  }

  // Cleans the raw data
  def cleanData(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.na.drop(Seq("MEMBER"))
      .withColumn("refill_date", to_date(col("SERVICE DATE"), "yyyy-MM-dd"))
      .withColumn("adherence_score", col("ADHERENCE").cast("double"))
      .withColumn("units", col("UNITS").cast("int"))
      .withColumn("amount_claimed", col("AMOUNT CLAIMED").cast("double"))
      .withColumn("script_code", upper(trim(col("SCRIPT CODE"))))
  }

  // Computes Proportion of Days Covered (PDC)
  def computePDC(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    df.groupBy($"MEMBER", $"script_code")
      .agg(
        min($"refill_date").as("start_date"),
        max($"refill_date").as("end_date"),
        count($"refill_date").as("refill_count")
      )
      .withColumn("total_days_supply", $"refill_count" * 30) // Assuming 30-day supply
      .withColumn("observation_period_days", datediff($"end_date", $"start_date") + 1)
      .withColumn("PDC", round($"total_days_supply" / $"observation_period_days", 2))
      .withColumn("adherence_flag", when($"PDC" < 0.8, "NON-ADHERENT").otherwise("ADHERENT"))
  }

  // Segments patients based on adherence score
  def adherenceSegment(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.withColumn("adherence_segment",
      when($"adherence_score" < 0.6, "Poor")
        .when($"adherence_score" < 0.8, "Moderate")
        .otherwise("Good")
    )
  }

  // Monthly Adherence Trend
  def monthlyAdherenceTrend(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.withColumn("month", date_format($"refill_date", "yyyy-MM"))
      .groupBy("month")
      .agg(avg("adherence_score").as("avg_monthly_adherence"))
      .orderBy("month")
  }
}
