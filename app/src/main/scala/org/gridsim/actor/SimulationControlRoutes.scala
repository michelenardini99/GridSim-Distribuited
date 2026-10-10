package org.gridsim.actor

import java.util.UUID
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.*
import spray.json.DefaultJsonProtocol.*
import spray.json.RootJsonFormat
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.util.Timeout
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityRef
import org.gridsim.actor.protocol.SimulationProtocol.*
import org.gridsim.actor.SimulationActor
import org.apache.pekko.actor.typed.ActorRef
import org.gridsim.dsl.scenarios.GridScenarioCatalog
import org.gridsim.core.simulation.{SimulationConf, SimulationSpeed}
import org.gridsim.core.model.Environment

import java.time.LocalDateTime
import scala.concurrent.duration.*
import scala.concurrent.Future
case class CreateSimulationRequest(preset: String)
case class SimulationResponse(id: String, preset: String)
case class MessageResponse(message: String)
case class UpdateSpeedRequest(speed: String)
case class UpdateTickRequest(deltaSeconds: Long)

object SimulationJsonFormats:
  implicit val createFormat: RootJsonFormat[CreateSimulationRequest] = jsonFormat1(CreateSimulationRequest.apply)
  implicit val responseFormat: RootJsonFormat[SimulationResponse] = jsonFormat2(SimulationResponse.apply)
  implicit val msgResponseFormat: RootJsonFormat[MessageResponse] = jsonFormat1(MessageResponse.apply)
  implicit val infoFormat: RootJsonFormat[SimulationRegistryActor.SimulationInfo] = jsonFormat2(SimulationRegistryActor.SimulationInfo.apply)
  implicit val listFormat: RootJsonFormat[SimulationRegistryActor.SimulationsList] = jsonFormat1(SimulationRegistryActor.SimulationsList.apply)
  implicit val speedReqFormat: RootJsonFormat[UpdateSpeedRequest] = jsonFormat1(UpdateSpeedRequest.apply)
  implicit val tickReqFormat: RootJsonFormat[UpdateTickRequest] = jsonFormat1(UpdateTickRequest.apply)

class SimulationControlRoutes(
    registry: ActorRef[SimulationRegistryActor.Command],
    entityRefFor: String => EntityRef[SimulationCommand]
)(implicit system: ActorSystem[_]):

  import SimulationJsonFormats._
  import system.executionContext
  implicit val timeout: Timeout = 3.seconds

  val routes: Route =
    pathPrefix("api" / "simulations") {
      concat(
        pathEnd {
          concat(
            get {
              val f: Future[SimulationRegistryActor.SimulationsList] = registry.ask(SimulationRegistryActor.GetSimulations(_))
              complete(f)
            },
            post {
              entity(as[CreateSimulationRequest]) { req =>
                GridScenarioCatalog.byId(req.preset) match {
                  case Some(preset) =>
                    val tickDelta = 1.hour
                    preset.build(tickDelta).build().toEither match {
                      case Right((model, state)) =>
                        val id = UUID.randomUUID().toString
                        val f: Future[SimulationRegistryActor.Registered] = registry.ask(SimulationRegistryActor.RegisterSimulation(id, req.preset, _))

                        onSuccess(f) { _ =>
                          val seededState = state.copy(environment = Environment(LocalDateTime.now()))
                          val conf = SimulationConf(delta = tickDelta)
                          val entityRef = entityRefFor(id)

                          // Initialize the simulation actor
                          entityRef.ask(replyTo => Initialize(model, seededState, conf, replyTo)).map { _ =>
                            entityRef ! Start
                          }

                          complete(SimulationResponse(id, req.preset))
                        }
                      case Left(errors) =>
                        complete(400 -> MessageResponse(s"Failed to build preset: ${errors}"))
                    }
                  case None =>
                    complete(404 -> MessageResponse(s"Preset '${req.preset}' not found"))
                }
              }
            }
          )
        },
        pathPrefix(Segment) { id =>
          concat(
            path("start") {
              post {
                val entityRef = entityRefFor(id)
                entityRef ! Start
                complete(MessageResponse(s"Start command sent to simulation $id"))
              }
            },
            path("status") {
              get {
                val entityRef = entityRefFor(id)
                val f: Future[String] = entityRef.ask(GetStatus(_))
                onSuccess(f) { status =>
                  complete(MessageResponse(status))
                }
              }
            },
            path("pause") {
              post {
                val entityRef = entityRefFor(id)
                entityRef ! Pause
                complete(MessageResponse(s"Pause command sent to simulation $id"))
              }
            },
            path("step") {
              post {
                val entityRef = entityRefFor(id)
                entityRef ! Step
                complete(MessageResponse(s"Step command sent to simulation $id"))
              }
            },
            path("speed") {
              post {
                entity(as[UpdateSpeedRequest]) { req =>
                  scala.util.Try(SimulationSpeed.valueOf(req.speed)).toOption match {
                    case Some(speed) =>
                      entityRefFor(id) ! UpdateSpeed(speed)
                      complete(MessageResponse(s"Speed update sent to simulation $id"))
                    case None =>
                      complete(400 -> MessageResponse(s"Unknown speed '${req.speed}'"))
                  }
                }
              }
            },
            path("tick") {
              post {
                entity(as[UpdateTickRequest]) { req =>
                  entityRefFor(id) ! UpdateTickDelta(req.deltaSeconds.seconds)
                  complete(MessageResponse(s"Tick delta update sent to simulation $id"))
                }
              }
            },
            pathEnd {
              delete {
                val entityRef = entityRefFor(id)
                entityRef ! Stop
                val f = registry.ask(SimulationRegistryActor.UnregisterSimulation(id, _))
                onSuccess(f) { _ =>
                  complete(MessageResponse(s"Simulation $id stopped and unregistered"))
                }
              }
            }
          )
        }
      )
    }
