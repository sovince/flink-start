package com.sovince.batch.transformation

import org.apache.flink.api.scala._

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/11
  * Time: 23:15
  * Description:
  */
object DataSetTransformationTest {
  private val env = ExecutionEnvironment.getExecutionEnvironment

  def mapAndFilter(): Unit ={
    val data = env.fromCollection(List(1,2,3,4,5,6,7,8,9,10))
//    data.mapPartition()
    data.map(_*10).filter(_>30).print()
  }

  def main(args: Array[String]): Unit = {
    mapAndFilter()
  }

}
