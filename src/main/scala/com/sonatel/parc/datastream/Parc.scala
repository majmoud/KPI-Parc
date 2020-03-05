package com.sonatel.parc.datastream

import com.sonatel.parc.session.CreateSession

object Parc {

  val spark = CreateSession.spark

  /*
    Function used to load the Json file and generate the sql request from a parc
   */
  def getRequest(file : String) : String = {
    val lRule = Functions.getRuleObject(s"$file")
    //try {
      val parcRule = spark.sparkContext.broadcast(lRule)
      // Recover the column selected and the wording
      val colparc = parcRule.value.colparc
      var groupby = ""
      for (b <- parcRule.value.groupby) { groupby = groupby + b + ", "}
      groupby = groupby.substring(0, groupby.length - 2)
      var andFilter = ""
      var request =
        s"""
           |SELECT DISTINCT date_transaction as DATE_TRANSACTION,
           |$colparc AS NUMBER, $groupby AS TRANSACTION_TAG FROM parc WHERE
           |""".stripMargin
      if (parcRule.value.filters.size > 0) { andFilter = " AND "}
      for (level1 <- parcRule.value.filters) {
        for (level2 <- level1.value) {
          val colSelected = level2.col
          val operator = level2.operator
          // Verified if the number of values is equal to 1
          var value2 = " "
          if (level2.value.size == 1) {
            val value1 = level2.value(0)
            request = request + colSelected + operator + s"'$value1'" + s" $andFilter "
          }
          // Recover all values
          else {
            value2 = value2 + " ("
            for (v <- level2.value) {
              value2 = value2 + s"'$v'" + ","
            }
            request = request + colSelected + s" $operator " + value2.substring(0, value2.length - 1) + ")" + s" $andFilter "
          }
        }
      }
      request.substring(0, request.length - 6)
    //}

  }
}
