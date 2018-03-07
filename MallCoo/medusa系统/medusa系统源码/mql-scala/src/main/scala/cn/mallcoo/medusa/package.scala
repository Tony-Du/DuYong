package cn.mallcoo

import net.ceedubs.ficus.SimpleFicusConfig
import net.ceedubs.ficus.readers.ValueReader

/**
  * Created by whimmy on 17/06/10.
  */
package object medusa extends net.ceedubs.ficus.readers.AllValueReaderInstances {
  implicit class FakePathSimpleFicusConfig(config: com.typesafe.config.Config) {
    def as[A](path: String)(implicit reader: ValueReader[A]): A = SimpleFicusConfig(config).as[A](path)

    def as[A](implicit reader: ValueReader[A]): A = SimpleFicusConfig(config.atKey("fakepath")).as[A]("fakepath")
  }
}
