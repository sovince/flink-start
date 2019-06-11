package com.sovince.batch.datasource

import com.sovince.pojo.StuPojo
import org.apache.flink.api.scala._
import org.junit.Test

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/10
  * Time: 21:06
  * Description:
  */
object DataSetSourceTest {

  private val env = ExecutionEnvironment.getExecutionEnvironment

  def readText(): Unit ={
    env.readTextFile("input/wordcount.txt").print()
  }

  def readDir(): Unit ={
    env.readTextFile("input/recursive").print()
  }

  def readCsvToTuple(): Unit ={
    env
      .readCsvFile[(String,Int)]("input/datasource/stu.csv",ignoreFirstLine=true,includedFields=Array(1,2))
      .print()
  }

  def readCsvToCaseClass(): Unit ={
    case class Stu(name:String,age:Int)
    env
      .readCsvFile[Stu]("input/datasource/stu.csv",ignoreFirstLine=true,includedFields=Array(1,2))
      .print()
  }

  def readCsvToPojo(): Unit ={
    env
      .readCsvFile[StuPojo]("input/datasource/stu.csv",ignoreFirstLine=true,pojoFields=Array("id","name","age"))
      .print()
  }

  def main(args: Array[String]): Unit = {
    readCsvToPojo()
  }


}
