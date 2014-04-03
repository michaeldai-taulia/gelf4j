/*
 * Copyright (c) 2011 - Philip Stehlik - p [at] pstehlik [dot] com
 * Licensed under Apache 2 license - See LICENSE for details
 */
package com.pstehlik.groovy.gelf4j.net

import com.pstehlik.groovy.gelf4j.appender.Gelf4JAppender
import com.pstehlik.groovy.graylog.Graylog2UdpSender
import com.pstehlik.groovy.graylog.Graylog2TcpSender
import java.nio.ByteBuffer

/**
 * Transports GELF messages over the wire to a graylog2 server based on the config on a <code>Gelf4JAppender</code>
 * ID generation based on the Hibernate UUIDHexGenerator
 *
 * @author Philip Stehlik
 * @since 0.7
 */
class GelfTransport {
  private static final USED_CHARSET = 'UTF-8'
  private static short counter = (short) 0
  private static final int JVM = (int) (System.currentTimeMillis() >>> 8)
  private static final int IP

  static {
    int ipadd
    try {
      ipadd = BytesHelper.toInt(InetAddress.getLocalHost().getAddress())
    }
    catch (Exception e) {
      ipadd = 0
    }
    IP = ipadd;
  }

  private String sep = ""

  public void sendGelfMessageToGraylog(Gelf4JAppender appender, String gelfMessage, String protocol) {
    if (protocol == "udp") {
      this.sendGelfMessageToGraylogUdp(appender, gelfMessage)
    } else {
      this.sendGelfMessageToGraylogTcp(appender, gelfMessage)
    }
  }
      
  /**
   * GZips the <code>gelfMessage</code> and sends it to the server as indicated on the <code>appender</code>
   *
   * @param appender Appender to use for the configuration retrieval of where to send the message to
   * @param gelfMessage The message to send
   */
  public void sendGelfMessageToGraylogUdp(Gelf4JAppender appender, String gelfMessage) {
    byte[] gzipMessage = gzipMessage(gelfMessage)

    //set up chunked transfer if larger than maxChunkSize
    if (appender.maxChunkSize < gzipMessage.size()) {

      Integer chunkCount = (((gzipMessage.size() / appender.maxChunkSize) + 0.5) as Double).round().toInteger()
      String messageId = generateMessageId()
      chunkCount.times {
        byte[] messageChunkPrefix = createChunkedMessagePart(messageId, it + 1, chunkCount)
        Integer endOfChunk
        Integer messagePartSize
        if (gzipMessage.size() < ((it + 1) * appender.maxChunkSize)) {
          endOfChunk = gzipMessage.size()
          messagePartSize = gzipMessage.size() - (appender.maxChunkSize * it)
        } else {
          endOfChunk = (appender.maxChunkSize * (it + 1))
          messagePartSize = appender.maxChunkSize
        }

        byte[] chunkedMessagPart = new byte[messageChunkPrefix.size() + messagePartSize]
        System.arraycopy(
          messageChunkPrefix,
          0,
          chunkedMessagPart,
          0,
          messageChunkPrefix.size()
        )
        System.arraycopy(
          gzipMessage,
          endOfChunk - messagePartSize,
          chunkedMessagPart,
          messageChunkPrefix.size(),
          messagePartSize
        )
        Graylog2UdpSender.sendPacket(
          chunkedMessagPart,
          appender.graylogServerHost,
          appender.graylogServerPort
        )
      }
    } else {
      Graylog2UdpSender.sendPacket(gzipMessage, appender.graylogServerHost, appender.graylogServerPort)
    }
  }

  /**
  * No gzip for transport over TCP as there are some issues. 
  *
  * @param appender Appender to use for the configuration retrieval of where to send the message to
  * @param gelfMessage The message to send
  */
  public void sendGelfMessageToGraylogTcp(Gelf4JAppender appender, String gelfMessage) {
    byte[] messageBytes
    gelfMessage += '\0'
    messageBytes = gelfMessage.getBytes("UTF-8")
    ByteBuffer buffer = ByteBuffer.allocate(messageBytes.length)
    buffer.put(messageBytes)
    buffer.flip()

    Graylog2TcpSender.sendPacket(
      buffer.array(),
      appender.graylogServerHost,
      appender.graylogServerPort
    )
  }

  /**
   * Creates the bytes that identify a chunked message.
   *
   * @param messageId The unique identifier for this message
   * @param sequenceNo The number of the message in the chunk sequence
   * @param sequenceCount The total number of chunks
   * @return The prepared byte array
   */
  private byte[] createChunkedMessagePart(String messageId, Integer sequenceNo, Integer sequenceCount) {
    sequenceNo = sequenceNo -1
    Collection ret = []
    ret << 30.byteValue()
    ret << 15.byteValue()
    messageId.getBytes('ISO-8859-1').each {
      ret << it
    }

    ret << (byte)(sequenceNo >>> 8)
    ret << (byte)(sequenceNo)

    ret << (byte)(sequenceCount >>> 8)
    ret << (byte)(sequenceCount)
    return ret as byte[]
  }

  /**
   * GZips a given message
   *
   * @param message
   * @return
   */
  private byte[] gzipMessage(String message) {
    def targetStream = new ByteArrayOutputStream()
    def zipStream = new java.util.zip.GZIPOutputStream(targetStream)
    zipStream.write(message.getBytes(USED_CHARSET))
    zipStream.close()
    def zipped = targetStream.toByteArray()
    targetStream.close()
    return zipped
  }

  /**
   * Generate unique Message ID
   * @return
   */
  private String generateMessageId() {
    return new StringBuffer(36)
      .append(format(getIP()))
      .append(sep)
      .append(format(getJVM()))
      .append(sep)
      .append(format(getHiTime()))
      .append(sep)
      .append(format(getLoTime()))
      .append(sep)
      .append(format(getCount())).toString()
  }

  protected String format(int intval) {
    String formatted = Integer.toHexString(intval)
    StringBuffer buf = new StringBuffer("00000000")
    buf.replace(8 - formatted.length(), 8, formatted)
    return buf.toString()
  }

  protected static String format(short shortval) {
    String formatted = Integer.toHexString(shortval)
    StringBuffer buf = new StringBuffer("0000")
    buf.replace(4 - formatted.length(), 4, formatted)
    return buf.toString()
  }

  /**
   * Unique across JVMs on this machine (unless they load this class
   * in the same quater second - very unlikely)
   */
  protected int getJVM() {
    return JVM
  }

  /**
   * Unique in a millisecond for this JVM instance (unless there
   * are > Short.MAX_VALUE instances created in a millisecond)
   */
  protected short getCount() {
    synchronized (this.class) {
      if (counter < 0) counter = 0
      return counter++
    }
  }

  /**
   * Unique in a local network
   */
  protected int getIP() {
    return IP
  }

  /**
   * Unique down to millisecond
   */
  protected short getHiTime() {
    return (short) (System.currentTimeMillis() >>> 32)
  }

  protected int getLoTime() {
    return (int) System.currentTimeMillis()
  }

  protected String format(int intval, int lastDigits) {
    String formatted = Integer.toHexString(intval)
    formatted = formatted.substring(0, formatted.size() >= lastDigits ? lastDigits : formatted.size())
    return formatted.padLeft(lastDigits, '0')
  }

  protected static String format(short shortval, int lastDigits) {
    String formatted = Integer.toHexString(shortval)
    formatted = formatted.substring(0, formatted.size() >= lastDigits ? lastDigits : formatted.size())
    return formatted.padLeft(lastDigits, '0')
  }
}
