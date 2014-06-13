package com.tinkerpop.gremlin.server.simulation

import io.gatling.core.Predef._
import scala.concurrent.duration._
import akka.actor.ActorRef;
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.action.{Chainable, system}
import bootstrap._
import assertions._
import akka.actor.Props
import io.gatling.core.result.message.{RequestMessage, KO, OK}
import io.gatling.core.result.writer.DataWriter
import com.tinkerpop.gremlin.driver.{Client, Cluster}

class GremlinServerSimulation extends Simulation {

  val mine = new ActionBuilder {
    def build(next: ActorRef) = system.actorOf(Props(new GremlinServerAction(next)))
  }

  val usersToSimulate = Integer.getInteger("users", 1).intValue()
  val simulationTime  = Integer.getInteger("time", 30).intValue()

  val scn = scenario("Gremlin Server Test").repeat(2) { exec(mine) }
  setUp(scn.inject(ramp(usersToSimulate users) over (simulationTime seconds)))
    .assertions(global.responseTime.max.lessThan(250),global.successfulRequests.percent.greaterThan(95))
}

class GremlinServerAction(val next: ActorRef) extends Chainable {
  val cluster: Cluster = Cluster.open
  val client: Client = cluster.connect

  def send(session: Session) {
    client.submit("1+1")
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