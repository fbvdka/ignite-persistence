package test.db

import javax.cache.Cache
import com.typesafe.config.ConfigFactory
import org.apache.ignite.{IgniteBinary, Ignition}
import org.apache.ignite.binary.{BinaryObject, BinaryObjectBuilder}
import org.apache.ignite.cache.store.CacheStoreAdapter
import org.apache.ignite.lang.IgniteBiInClosure
import org.slf4j.LoggerFactory
import slick.jdbc.PostgresProfile
import slick.jdbc.PostgresProfile.api._
import slick.lifted
import test.nodes.Device

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

trait BinaryPostgresSlickConnection {

  val pgProfile = PostgresProfile.api

  val pgDatabase = Database.forConfig("ignitePostgres")

  val tableName: String
}

class CacheBinaryPostgresSlickStore extends CacheStoreAdapter[String, Any] with BinaryPostgresSlickConnection with Serializable {

  import scala.concurrent.ExecutionContext.Implicits.global

  val config = ConfigFactory.load("application.conf")

  val log = LoggerFactory.getLogger("IgniteLog")

  val tableName = "device_ignite_slick_table_binary"

  import pgProfile._

  //Slick

  class DeviceTable(tag: Tag) extends Table[Device](tag, Some("public"), tableName) {

    def id = column[String]("id")

    def metadata = column[String]("metadata")

    def lat = column[Double]("lat")

    def lon = column[Double]("lon")

    def * = (id, metadata, lat, lon) <> (Device.tupled, Device.unapply)

    def pk = primaryKey(s"pk_$tableName", id)

    def uniqueIndex = index(s"idx_$table", id, unique = true)
  }

  val table = lifted.TableQuery[DeviceTable]

  val startup = createSchema

  private def createSchema(): Unit = {
    pgDatabase.run(table.exists.result) onComplete {
      case Success(exists) =>
        log.info("Schema already exists")
      case Failure(e) => {
        log.info(s"Creating schema for $tableName")
        val dbioAction = (
          for {
            _ <- table.schema.create
          } yield ()
          ).transactionally
        pgDatabase.run(dbioAction)
      }
    }
  }

  override def loadCache(clo: IgniteBiInClosure[String, Any], args: AnyRef*): Unit = {
    for {
      devices <- pgDatabase.run(table.map(u => u).result) recoverWith { case _ => Future(Seq.empty[Device]) }
    } yield {
      log.info(s"Loading cache $tableName")
      devices.foreach(device => clo.apply(device.id, Ignition.ignite().binary().toBinary(device)))
    }
  }

  override def delete(key: Any) = Try {
    log.info(s"Delete from $tableName value $key")
    val dbioAction = DBIO.seq(
      table.filter(_.id === key.toString).delete
    ).transactionally
    pgDatabase.run(dbioAction)
  }

  override def write(entry: Cache.Entry[_ <: String, _ <: Any]): Unit = Try {
    val device: Device = entry.getValue.asInstanceOf[BinaryObject].deserialize()
    log.info(s"Insert into $tableName value ${device.toString}")
    val dbioAction = DBIO.seq(table.insertOrUpdate(device)).transactionally
    pgDatabase.run(dbioAction)
  }

  override def load(key: String): Any = {
    val loadedDevice = pgDatabase.run(table.filter(_.id === key).result.headOption)
    val device = Await.result(loadedDevice, 10 second).getOrElse(null)
    Ignition.ignite().binary().toBinary(device)
  }


}