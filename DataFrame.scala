package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object DataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()  
      .appName("job-1")  
      .master("local[2]")  
      .getOrCreate()

  val df = spark.read  
      .option("header", "true")  
      .option("delimiter", ";")  
      .option("inferSchema", "true")  
      .csv("src/main/resources/codesPostaux.csv")  
      //.format("csv")  //.load("C:/Users/hadij/Downloads/spark-df.tar.gz_Dossier/spark-df/src/main/resources/codesPostaux.csv")

  
  // Affiche le schéma
  df.printSchema
  // Affiche le nombre d'éléments unique de chaque colonnes 
  val exprs = df.columns.map((_ -> "approx_count_distinct")).toMap
  df.agg(exprs).show()

  val new_df = df.withColumn("departement", (col("Code_commune_INSEE") / 1000).cast("integer"))
  
  // Affiche le nombre d'éléments unique de chaque colonnes 
  val new_exprs = new_df.columns.map((_ -> "approx_count_distinct")).toMap
  new_df.agg(exprs).show()
  
  val orderedData = new_df.orderBy("Code_postal")
  orderedData.write.format("csv").save("commune_et_departement.csv")

  val aisne = new_df.select("Nom_commune").filter("departement = 2")
  aisne.foreach(x => println(x))

  val dep = new_df
  .groupBy( "departement")
  .agg(count("Nom_commune").as("nb_communes"))
  dep.sort(col("nb_communes").desc)
  .show(1)
  
  }


}
