/*
 * Copyright (c) 2014 - Philip Stehlik - p [at] pstehlik [dot] com
 * Licensed under Apache 2 license - See LICENSE for details
 */
package com.pstehlik.groovy.graylog

import org.apache.log4j.helpers.LogLog

/**
 * Connection handling for sending data to Graylog
 *
 * @author Michael Dai
 * @since 0.9.6
 */
class Graylog2TcpSender {
  /**
   * Sends data via TCP to a host at a given port
   *
   * @param data
   * @param hostname
   * @param port
   */
  public static void sendPacket(byte[] data, String hostname, Integer port){
    Socket clientSocket = new Socket();
    SocketAddress address = new InetSocketAddress(hostname, port)
    try{
      clientSocket.connect(address, port);
      DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
      outToServer.write(data);
    } catch(UnknownHostException ex) {
      LogLog.error("Could not determine address for [${hostname}]")
    } catch(IOException ex) {
      LogLog.error("Error when sending data to [${address}/${port}]. [${ex.message}]")
    } finally {
      clientSocket.close();
    }
  }
}
