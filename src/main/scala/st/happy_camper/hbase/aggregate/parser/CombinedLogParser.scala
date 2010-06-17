package st.happy_camper.hbase.aggregate.parser

import _root_.st.happy_camper.hbase.aggregate.CombinedLog

import _root_.java.util.{ Date, Locale }
import _root_.java.text.SimpleDateFormat

import _root_.scala.util.parsing.combinator.RegexParsers

object CombinedLogParser extends RegexParsers {
  override val whiteSpace = "".r

  lazy val parse: Parser[CombinedLog] = 
    remoteHost ~ " - " ~ remoteUser ~ " " ~ requestedTime ~ " \"" ~ method ~ " " ~ requestPath ~ " " ~ protocol ~ "\" " ~ statusCode ~ " " ~ contentLength ~ " " ~ referer ~ " " ~ userAgent ^^ {
      case remoteHost ~ " - " ~ remoteUser ~ " " ~ requestedTime ~ " \"" ~ method ~ " " ~ requestPath ~ " " ~ protocol ~ "\" " ~ statusCode ~ " " ~ contentLength ~ " " ~ referer ~ " " ~ userAgent =>
        CombinedLog(remoteHost, remoteUser, requestedTime, method, requestPath, protocol, statusCode, contentLength, referer, userAgent)
    }

  lazy val remoteHost: Parser[String] = "[^ ]+".r
  lazy val remoteUser: Parser[Option[String]] = "[^ ]+".r ^^ {
    case "-" => None
    case x   => Some(x)
  }
  lazy val requestedTime: Parser[Date] = elem('[') ~> "[^]]+".r <~ elem(']') ^^ {
    new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US).parse(_)
  }

  lazy val method:      Parser[String] = "[A-Z]+".r
  lazy val requestPath: Parser[String] = "[^ ]+".r
  lazy val protocol:    Parser[String] = "[^\"]+".r

  lazy val statusCode: Parser[Int] = "[0-9]+".r ^^ { Integer.parseInt(_) }

  lazy val contentLength: Parser[Option[Int]] = "[0-9]+|-".r ^^ {
    i =>
      try { Some(Integer.parseInt(i)) } catch { case e => None }
  }
  lazy val referer: Parser[Option[String]] =
    "\"" ~> "[^\"]*".r <~ "\"" ^^ {
      r =>
        if(r != "" && r != "-") { Some(r) } else { None }
    }
  lazy val userAgent: Parser[Option[String]] =
    "\"" ~> "(?:\\\\\"|[^\"])*".r <~ "\"" ^^ {
      ua =>
        if(ua != "") { Some(ua.replaceAll("\\\\\"", "\"")) } else { None }
    }
}
