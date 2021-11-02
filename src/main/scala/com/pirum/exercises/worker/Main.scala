package com.pirum.exercises.worker

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern.{Askable, _}
import akka.util.Timeout

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future}

object Main extends App with Program {
  def program(
      tasks: List[Task],
      timeout: FiniteDuration,
      workers: Int
  ): Unit = {
    println(runTasks(tasks, timeout, workers))
  }

  def runTasks(
      tasks: List[Task],
      timeout: FiniteDuration,
      workers: Int
  ): AggregatedResult = {

    // This allows aggregation of results to occur after timeout has occurred
    // Full second is not needed
    val actorSystemTimeOut = timeout + 1.second

    implicit val system: ActorSystem[MainActor.GetResult] =
      ActorSystem(MainActor(tasks, workers, timeout), "mainActor")
    implicit val ec = system.executionContext
    implicit val implicitTimeout: Timeout = actorSystemTimeOut

    val result: Future[MainActor.Result] =
      system.ask(ref => MainActor.GetResult(ref))

    Await.result(result, actorSystemTimeOut).aggregatedResult

  }
}
