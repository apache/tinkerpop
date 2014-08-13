package com.tinkerpop.gremlin.server.simulation

import io.gatling.core.Predef._
import scala.concurrent.duration._
import akka.actor.ActorRef
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.action.Chainable
import akka.actor.Props
import io.gatling.core.result.message.{KO, OK}
import io.gatling.core.result.writer.{RequestMessage, DataWriter}
import com.tinkerpop.gremlin.driver.{Client, Cluster}

class GremlinServerAdditionSimulation extends Simulation {
  val host: String = System.getProperty("host", "localhost")
  val maxConnectionPoolSize: Integer = Integer.getInteger("maxConnectionPoolSize", 64)
  val cluster: Cluster = Cluster.create(host)
    .maxConnectionPoolSize(maxConnectionPoolSize)
    .build()

  val addition = new ActionBuilder {
    def build(next: ActorRef) = system.actorOf(Props(new GremlinServerAction(next, cluster, "1+1")))
  }

  val randomNumberedRequests = scenario("Random Numbered Requests").randomSwitch(
    50 -> repeat(100) {
      exec(addition)
    },
    50 -> repeat(200) {
      exec(addition)
    }
  )

  val fixedRequests = scenario("Fixed Requests").repeat(5) {
    exec(addition)
  }

  setUp(
    fixedRequests.inject(
      constantRate(1000 userPerSec) during(60 seconds),
      nothingFor(5 seconds),
      ramp(5000 users) over (60 seconds)),
    randomNumberedRequests.inject(
      nothingFor(180 seconds),
      split(1000 users).into(ramp(100 users) over (25 seconds)).separatedBy(1 seconds))
  ).assertions(global.responseTime.max.lessThan(250), global.successfulRequests.percent.greaterThan(95))
}

class GremlinServerAction(val next: ActorRef, val cluster: Cluster, val script: String) extends Chainable {
  val client: Client = cluster.connect

  def send(session: Session) {
    client.submitAsync(script)
  }

  def execute(session: Session) {
    var start: Long = 0L
    var end: Long = 0L
    var status: Status = OK
    var errorMessage: Option[String] = None

    try {
      start = System.currentTimeMillis
      send(session)
      end = System.currentTimeMillis
    } catch {
      case e: Exception =>
        errorMessage = Some(e.getMessage)
        logger.error("Stinks - that FAILED", e)
        status = KO
    } finally {
      DataWriter.tell(
        RequestMessage(session.scenarioName, session.userId, session.groupStack, "Gremlin Server Test Scenario",
          start, start, end, end,
          status, errorMessage, Nil))
      next ! session
    }
  }
}