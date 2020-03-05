package com.sonatel.parc.datastream
import java.io.File

import com.sonatel.parc.datastream.Parc.spark

import scala.collection.mutable.ListBuffer

object SelectFilesParcs extends App {

  /*
    Function used to select all json files for all parcs.
   */
  def getFiles(dir : String) : List[File] = {
    val d = new File(dir)
    if (d.exists() && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.filter {
        file => file.getName.endsWith(".json")
      }
    }
    else {
      List[File]()
    }
  }

  /*
    Function used to select all parcs names
   */
  def getNames() = {

  }

}
