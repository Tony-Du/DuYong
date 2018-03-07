

import javax.sql.DataSource

import com.typesafe.config.{Config, ConfigFactory}
import com.zaxxer.hikari.HikariDataSource
import scalikejdbc.{ConnectionPool, DataSourceConnectionPool}
import scalikejdbc._
import scalikejdbc.config._

/**
  * Created by whimmy on 17/06/18.
  */
object client {

  def main(args: Array[String]): Unit = {

    DBsWithConfigFileAndEnv("scalike")("test").setupAll()
//    val dataSource: DataSource = {
//      val ds = new HikariDataSource()
//      ds.setJdbcUrl("com.cloudera.impala.jdbc4.Driver")
//      ds.setJdbcUrl("jdbc:impala://115.29.204.19:21050/medusa_shop")
//      ds.setMaximumPoolSize(5)
//      ds.setMaxLifetime(60 * 60 * 1000)
//      ds.setUsername("medusa")
//      ds.setConnectionTestQuery("select 1")
//      ds
//    }
//    ConnectionPool.singleton(new DataSourceConnectionPool(dataSource))

    val count = DB readOnly { implicit session =>
      sql"""SELECT count(*) FROM medusa_shop.kudu_event""".map(rs => rs.long(1)).single.apply()
    }



    println(count)

        DBs.closeAll()

    //    val dataSource = new HikariDataSource()
    //    dataSource.setJdbcUrl("jdbc:impala://115.29.204.19:21050/medusa_shop")
    //    dataSource.setDriverClassName("com.cloudera.impala.jdbc4.Driver")
    //    classOf[com.cloudera.impala.jdbc4.Driver]

    //    dataSource.setMaximumPoolSize(50)
    //    dataSource.setMaxLifetime(60 * 60 * 1000)
    //    dataSource.setUsername("medusa")
    //    dataSource.setConnectionTestQuery("select 1")

    //    val con = dataSource.getConnection()

    //    val con = DriverManager.getConnection("jdbc:impala://115.29.204.19:21050/db_1")
    //    val ps = con.prepareStatement("select mallcoo_id,distinct_id from medusa_shop.kudu_user limit 10;")
    //    val rs = ps.executeQuery()
    //    while (rs.next()) {
    //      println(rs.getString("mallcoo_id") + "\t" + rs.getString("distinct_id"))
    //    }
    //
    //    con.close()
    //
    //    dataSource.close()
  }


}


case class DBsWithConfigFileAndEnv(file: String)(envValue: String) extends DBs
  with TypesafeConfigReader
  with TypesafeConfig
  with EnvPrefix {

  lazy val config: Config = ConfigFactory.load(file)
  override val env = Option(envValue)
}