package st.happy_camper.hbase.aggregate

import _root_.java.io.{ DataInput, DataOutput }
import _root_.java.util.Date

import _root_.org.apache.hadoop.io.{ Text, Writable, WritableUtils }

class CombinedLogWritable(var combinedLog: Option[CombinedLog]) extends Writable {

  def this() = this(None)

  def this(combinedLog: CombinedLog) = this(Option(combinedLog))

  def write(out: DataOutput) {
    combinedLog map {
      combinedLog => {
        Text.writeString(out, combinedLog.remoteHost)
        combinedLog.remoteUser match {
          case Some(remoteUser) => {
            out.writeBoolean(true)
            Text.writeString(out, remoteUser)
          }
          case None => 
            out.writeBoolean(false)
        }
        WritableUtils.writeVLong(out, combinedLog.requestedTime.getTime)
        Text.writeString(out, combinedLog.method)
        Text.writeString(out, combinedLog.requestPath)
        Text.writeString(out, combinedLog.protocol)
        WritableUtils.writeVInt(out, combinedLog.statusCode)
        combinedLog.contentLength match {
          case Some(contentLength) => {
            out.writeBoolean(true)
            WritableUtils.writeVInt(out, contentLength)
          }
          case None => 
            out.writeBoolean(false)
        }
        combinedLog.referer match {
          case Some(referer) => {
            out.writeBoolean(true)
            Text.writeString(out, referer)
          }
          case None => 
            out.writeBoolean(false)
        }
        combinedLog.userAgent match {
          case Some(userAgent) => {
            out.writeBoolean(true)
            Text.writeString(out, userAgent)
          }
          case None => {
            out.writeBoolean(false)
          }
        }
      }
    }
  }

  def readFields(in: DataInput) {
    combinedLog = Some(CombinedLog(
      Text.readString(in),
      if(in.readBoolean) Some(Text.readString(in)) else None,
      new Date(WritableUtils.readVLong(in)),
      Text.readString(in),
      Text.readString(in),
      Text.readString(in),
      WritableUtils.readVInt(in),
      if(in.readBoolean) Some(WritableUtils.readVInt(in)) else None,
      if(in.readBoolean) Some(Text.readString(in)) else None,
      if(in.readBoolean) Some(Text.readString(in)) else None
    ))
  }
}
