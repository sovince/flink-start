package com.sovince.stream.datasink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/15
  * Time: 14:11
  * Description:
  */
class Sink2MysqlCity extends RichSinkFunction[City] {

  private var connection: Connection = _
  private var prepareStatement: PreparedStatement = _

  private def getConnection(): Connection = {
    classOf[com.mysql.jdbc.Driver]
    DriverManager.getConnection(
      "jdbc:mysql://localhost:3306/test",
      "root",
      "123123"
    )
  }


  override def open(parameters: Configuration): Unit = {
    connection = getConnection()
    prepareStatement = connection.prepareStatement("insert into city(code,name) value(?,?)")

  }


  override def invoke(value: City, context: SinkFunction.Context[_]): Unit = {
    prepareStatement.setInt(1,value.code)
    prepareStatement.setString(2,value.name)
    prepareStatement.executeUpdate()
  }

  override def close(): Unit = {
    prepareStatement.close()
    connection.close()
  }
}
