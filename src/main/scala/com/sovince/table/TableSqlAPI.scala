package com.sovince.table

import com.sovince.stream.datasink.City
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/15
  * Time: 15:52
  * Description:
  */
object TableSqlAPI {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val bTableEnv = BatchTableEnvironment.create(env)

    val csv = env.readCsvFile[City]("input/city.csv",ignoreFirstLine=true)
//      .print()
//    val cityTable = bTableEnv.fromDataSet(csv)
//    bTableEnv.registerTable("city",cityTable)

    bTableEnv.registerDataSet("city",csv)

    val resultTable = bTableEnv.sqlQuery("select sum(code) sum_code from city")

    bTableEnv.toDataSet[Row](resultTable).print()


  }
}
