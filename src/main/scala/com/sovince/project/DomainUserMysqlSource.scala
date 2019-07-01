package com.sovince.project

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/7/1
  * Time: 21:33
  * Description:
  */
class DomainUserMysqlSource extends RichParallelSourceFunction[util.HashMap[String,String]]{
  var connection:Connection = null
  var preparedStatement:PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://bigdata000:3306/flink"
    val user = "root"
    val password = "123123"
    connection = DriverManager.getConnection(url,user,password)
    val sql = "select domain,user from domain_user;"
    //获取执行语句
    preparedStatement = connection.prepareStatement(sql)
  }

//  override def close(): Unit = {
//
//  }

  override def run(ctx: SourceFunction.SourceContext[util.HashMap[String, String]]): Unit = {
    val resultSet = preparedStatement.executeQuery()
    val data = new util.HashMap[String, String]()

    while (resultSet.next()){
      data.put(resultSet.getString("domain"),resultSet.getString("user"))
    }
    ctx.collect(data)
  }

  override def cancel(): Unit = {
    if(preparedStatement!=null){
      preparedStatement.close()
    }
    if(connection!=null){
      connection.close()
    }
  }
}
