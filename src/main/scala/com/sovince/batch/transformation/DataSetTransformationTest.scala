package com.sovince.batch.transformation

import com.sovince.util.DbFakeUtil
import org.apache.flink.api.common.operators.Order
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

  //过滤
  def mapAndFilter(): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //    data.mapPartition()
    data.map(_ * 10).filter(_ > 30).print()
  }

  //分区
  def mapPartition(): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    data.mapPartition(x => {
      val con = DbFakeUtil.getConnection
      println(con)
      println("???")
      //x是一个集合collection
      x
    }).print()
  }

  //实现topN
  def firstFunction(): Unit = {
    val data = env.fromCollection(
      List(
        (1, "Hadoop"), (1, "flink"), (1, "spark"),
        (2, "Scala"), (2, "Java"), (2, "PHP"),
        (3, "Spring"), (4, "Panda")
      )
    )
//    data.first(4).print()
    data.groupBy(0).sortGroup(1,Order.DESCENDING).first(2).print()
  }

  def distinctFunction(): Unit ={
    val data = env.fromCollection(
      List(
        (1, "Hadoop"), (1, "flink"), (1, "spark"),
        (2, "Scala"), (2, "Java"), (2, "PHP"),
        (3, "Spring"), (4, "Panda")
      )
    )
    data.distinct(0).print()
  }

  def joinFunction(): Unit ={
    val list1 = List((1,"Anne",1), (2,"Bob",2), (3,"Cindy",3), (4,"Devin",5))
    val list2 = List((1,"Hangzhou"),(2,"Beijing"),(3,"Shanghai"),(4,"Shenzhen"))
    val students = env.fromCollection(list1)
    val citys = env.fromCollection(list2)

    students.join(citys).where(2).equalTo(0).apply((left,right)=>{
      (left._1,left._2,right._2)
    }).print()

    println("--------------")

    students.leftOuterJoin(citys).where(2).equalTo(0).apply((left,right)=>{
      if(right==null){
        (left._1,left._2,null)
      }else{
        (left._1,left._2,right._2)
      }
//      (
//        left._1,
//        left._2,
//        if(right==null) null else right._2
//      )

    }).print()

  }

  def main(args: Array[String]): Unit = {
    joinFunction()
  }

}
