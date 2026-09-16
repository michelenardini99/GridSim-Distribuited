package org.gridsim.core.actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.gridsim.core.behaviour.{EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.common.{Energy, Flow}
import org.gridsim.core.model.{Environment, GridEntity, GridEntityState}

import scala.concurrent.duration.FiniteDuration

object EntityActor:

  sealed trait EntityCommand

  final case class Evolve(
    env: Environment,
    delta: FiniteDuration,
    replyTo: ActorRef[EntityEvolved]
  ) extends EntityCommand
  
  final case class EntityEvolved(
    id: String, 
    state: GridEntityState, 
    flow: Flow[Energy]
  )

  def apply(
    entityState: GridEntityState,
    entity: GridEntity,
    handler: EntityEvolutionHandler
  ): Behavior[EntityCommand] = Behaviors.setup { context =>
    running(entityState, entity, handler)
  }

  private def running(state: GridEntityState, entity: GridEntity, handler: EntityEvolutionHandler): Behavior[EntityCommand] =
    Behaviors.receiveMessage{
      case Evolve(env, delta, replyTo) => 
        val request = EvolutionRequest(entity = entity, state = state, env = env, delta = delta)
        val (newState, flow) = handler.evolve(request)
        
        replyTo ! EntityEvolved(entity.id, newState, flow)
        
        running(newState, entity, handler)
    }
