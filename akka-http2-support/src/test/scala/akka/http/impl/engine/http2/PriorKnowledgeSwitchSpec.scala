/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.http.impl.engine.http2

import scala.concurrent.Promise

import akka.Done
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import akka.util.ByteString

import akka.testkit.AkkaSpec
import org.scalatest.exceptions.TestFailedException
import org.scalatest.time.{ Span, Second, Milliseconds }

class PriorKnowledgeSwitchSpec extends AkkaSpec {
  implicit val mat = ActorMaterializer()

  override val patience: PatienceConfig = PatienceConfig(timeout = Span(1, Second), interval = Span(50, Milliseconds))

  "The PriorKnowledgeSwitch" should {
    "switch to http2 when the connection preface arrives separately from the payload" in {
      val payload = ByteString("dfadfasdfa")
      val http1flowMaterialized = Promise[Done]()
      val http2flowMaterialized = Promise[Done]()
      val (in, out) = Source.queue[ByteString](100, OverflowStrategy.fail)
        .toMat(Sink.queue[ByteString])(Keep.both)
        .run()

      val queue = Source.queue(100, OverflowStrategy.fail)
        .viaMat(PriorKnowledgeSwitch(
          Flow[ByteString].mapMaterializedValue(_ => http1flowMaterialized.success(Done)),
          Flow[ByteString]
            .map(bs => { in.offer(bs); bs })
            .mapMaterializedValue(_ => http2flowMaterialized.success(Done))
        ))(Keep.left)
        .toMat(Sink.ignore)(Keep.left)
        .run()

      queue.offer(Http2Protocol.ClientConnectionPreface).futureValue should be(Enqueued)
      queue.offer(payload).futureValue should be(Enqueued)

      assertThrows[TestFailedException] {
        http1flowMaterialized.future.futureValue
      }
      http2flowMaterialized.future.futureValue should be(Done)
      out.pull.futureValue should be(Some(Http2Protocol.ClientConnectionPreface))
      out.pull.futureValue should be(Some(payload))
    }

    "switch to http2 when the connection preface arrives together with the payload" in {
      val payload = ByteString("dfadfasdfa")
      val http1flowMaterialized = Promise[Done]()
      val http2flowMaterialized = Promise[Done]()
      val (in, out) = Source.queue[ByteString](100, OverflowStrategy.fail)
        .toMat(Sink.queue[ByteString])(Keep.both)
        .run()

      val queue = Source.queue(100, OverflowStrategy.fail)
        .viaMat(PriorKnowledgeSwitch(
          Flow[ByteString].mapMaterializedValue(_ => http1flowMaterialized.success(Done)),
          Flow[ByteString]
            .map(bs => { in.offer(bs); bs })
            .mapMaterializedValue(_ => http2flowMaterialized.success(Done))
        ))(Keep.left)
        .toMat(Sink.ignore)(Keep.left)
        .run()

      queue.offer(Http2Protocol.ClientConnectionPreface ++ payload).futureValue should be(Enqueued)

      assertThrows[TestFailedException] {
        http1flowMaterialized.future.futureValue
      }
      http2flowMaterialized.future.futureValue should be(Done)
      out.pull.futureValue should be(Some(Http2Protocol.ClientConnectionPreface ++ payload))
    }

    "switch to http2 when the connection preface arrives in two parts" in {
      val http1flowMaterialized = Promise[Done]()
      val http2flowMaterialized = Promise[Done]()

      val queue = Source.queue(100, OverflowStrategy.fail)
        .viaMat(PriorKnowledgeSwitch(
          Flow[ByteString].mapMaterializedValue(_ => http1flowMaterialized.success(Done)),
          Flow[ByteString].mapMaterializedValue(_ => http2flowMaterialized.success(Done))
        ))(Keep.left)
        .toMat(Sink.ignore)(Keep.left)
        .run()

      queue.offer(Http2Protocol.ClientConnectionPreface.take(15)).futureValue should be(Enqueued)
      queue.offer(Http2Protocol.ClientConnectionPreface.drop(15)).futureValue should be(Enqueued)

      assertThrows[TestFailedException] {
        http1flowMaterialized.future.futureValue
      }
      http2flowMaterialized.future.futureValue should be(Done)
    }
  }
}
