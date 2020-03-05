package com.sonatel.parc.kpi

import com.sonatel.parc.datastream.{LoadFileTransaction, Parc, SelectFilesParcs}
import com.sonatel.parc.session.CreateSession

object Calcul extends App {

  val spark = CreateSession.spark
  // Create a dataframe. This dataframe will be used to merge all results
  import spark.implicits._
  var dfMerge =  Seq.empty[(String, String, String)].toDF("DATE_TRANSACTION", "NUMBER", "TRANSACTION_TAG")

  val fileSource = LoadFileTransaction.loadFileSource("OM_TRANSACTIONSUSERS_20200120_1.csv")
  fileSource.createOrReplaceTempView("parc")
  val filesJson = SelectFilesParcs.getFiles("src/main/resources/parcs")
  if (filesJson.size != 0) {
    try {
      for(i <- 0 to filesJson.length) {
        val file = filesJson(i)
        val request = Parc.getRequest(s"$file")
        val df = spark.sql(s"$request")
        // Merge the result to the dataframe
        dfMerge = df.union(dfMerge)
      }
    }
    catch {
      case ex : java.lang.IndexOutOfBoundsException => { println(
        """
          | ============================================================================
          ||         Traitement des fichiers effectué avec succès!!!                   |
          | ============================================================================
          |""".stripMargin)
      }
    }
    finally {
      dfMerge.show()
      //dfMerge.filter(dfMerge("NUMBER") === "784642811").show()
    }
  }
  else {
    println(
      """
        |Le repertoire contenant les fichiers Json est vide.
        |Veuillez verifier le chemin du repertoire.
        |""".stripMargin)
  }

}
