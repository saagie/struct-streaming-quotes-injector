package io.saagie.quotes

import scala.util.matching.Regex

object Quote extends Serializable {
  val pattern: Regex = """(\d+),[^,]*,\"([^"]+)\",[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,([^,]*),(\d+),.*""".r

  def parse(s: String): Option[Quote] = {
    try {
      pattern.findFirstMatchIn(s)
        .map(
          x => Quote(
            x.subgroups(1),
            x.subgroups.head.toLong / 1000L,
            x.subgroups(2).toDouble,
            x.subgroups(3).toLong
          )
        )
    } catch {
      case _: Throwable => None
    }
  }
}

case class Quote(symbol: String, ts: Long, price: Double, volume: Long)

case class QuoteVariation(label: String, price: Double, ts: Long, variation: Double)