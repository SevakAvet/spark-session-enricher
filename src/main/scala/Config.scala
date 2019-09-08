
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

object Config {
  private val config = ConfigFactory.load()

  val debug: Boolean = config.getBoolean("debug")
  val session: Int = config.getDuration("session", TimeUnit.SECONDS).toInt
  val topN: Int = config.getInt("topN")
  val inputFile: String = config.getString("inputFile")
}