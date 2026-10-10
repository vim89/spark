/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connect

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.connect.proto.{Command, ExecutePlanResponse}
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.util.CloseableIterator

class StreamingQueryListenerBusSuite extends ConnectFunSuite {
  private def newListener(): StreamingQueryListener = new StreamingQueryListener {
    override def onQueryStarted(event: QueryStartedEvent): Unit = {}
    override def onQueryProgress(event: QueryProgressEvent): Unit = {}
    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
  }

  Seq(false, true).foreach { registered =>
    test(s"remove ${if (registered) "a registered" else "an unregistered"} listener") {
      val commands = ArrayBuffer.empty[Command]
      val session = new SparkSession(SparkConnectClient.builder().build(), new AtomicLong()) {
        override def execute(command: Command): Seq[ExecutePlanResponse] = {
          commands += command
          Seq.empty
        }
      }
      session.releaseSessionOnClose = false
      val started = new CountDownLatch(1)
      val finish = new CountDownLatch(1)
      val stopped = new CountDownLatch(1)
      val bus = new StreamingQueryListenerBus(session) {
        override def registerServerSideListener(): CloseableIterator[ExecutePlanResponse] = {
          CloseableIterator(Iterator.empty)
        }

        override def queryEventHandler(iter: CloseableIterator[ExecutePlanResponse]): Unit = {
          started.countDown()
          try {
            finish.await()
          } catch {
            case _: InterruptedException => // Last-listener removal interrupts this worker.
          } finally {
            stopped.countDown()
          }
        }
      }
      val listener = newListener()
      try {
        bus.append(listener)
        assert(started.await(10, TimeUnit.SECONDS), "listener worker did not start")
        bus.remove(if (registered) listener else newListener())
        if (registered) {
          assert(commands.size == 1)
          assert(commands.head.getStreamingQueryListenerBusCommand.getRemoveListenerBusListener)
          assert(bus.list().isEmpty)
          assert(stopped.await(10, TimeUnit.SECONDS), "listener worker did not stop")
        } else {
          assert(commands.isEmpty, "an unregistered listener must not unregister the shared bus")
          assert(bus.list().toSeq == Seq(listener))
          assert(stopped.getCount == 1, "an unregistered listener must not interrupt the worker")
        }
      } finally {
        finish.countDown()
        try {
          bus.close()
          assert(stopped.await(10, TimeUnit.SECONDS), "listener worker did not stop")
        } finally {
          session.close()
        }
      }
    }
  }
}
