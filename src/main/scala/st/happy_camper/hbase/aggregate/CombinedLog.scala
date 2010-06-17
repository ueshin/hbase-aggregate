package st.happy_camper.hbase.aggregate

import _root_.java.util.Date

case class CombinedLog (
  remoteHost: String,
  remoteUser: Option[String],
  requestedTime: Date,
  method: String,
  requestPath: String,
  protocol: String,
  statusCode: Int,
  contentLength: Option[Int],
  referer: Option[String],
  userAgent: Option[String]
)
