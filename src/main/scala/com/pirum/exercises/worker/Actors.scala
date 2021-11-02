package com.pirum.exercises.worker

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import com.pirum.exercises.worker.Leader.RequestTask
import com.pirum.exercises.worker.MainActor.SendResult
import com.pirum.exercises.worker.Worker.DoTask

import scala.collection.immutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Future, TimeoutException}
import scala.util.{Failure, Success}

sealed trait TaskResult {
  val taskNumber: Int
}
case class SuccessfulResult(taskNumber: Int) extends TaskResult
case class FailedResult(taskNumber: Int) extends TaskResult

object MainActor {
  sealed trait Command
  sealed case class GetResult(replyTo: ActorRef[Result]) extends Command
  sealed case class SendResult(aggregatedResult: AggregatedResult)
      extends Command

  sealed trait Reply
  sealed case class Result(aggregatedResult: AggregatedResult) extends Reply

  def apply(
      tasks: Seq[Task],
      numWorkers: Int,
      timeout: FiniteDuration
  ): Behavior[Command] = {

    // Adds some slack so completed tasks can be sent to aggregator when timeout occurs
    val timeoutWithSlack = timeout + 50.milliseconds

    Behaviors.setup { context =>
      context.log.info("Starting MainActor")
      val aggregator =
        context.spawn(
          Aggregator(context.self, timeoutWithSlack, tasks.size),
          "aggregator"
        )
      val leader = context.spawn(Leader(tasks), "leader")
      (0 until numWorkers).foreach(i =>
        context.spawn(Worker(leader, aggregator, timeout), s"worker$i")
      )

      // A bit of a hack to glue the ask pattern to the aggregator pattern
      def manageResult(
          maybeResult: Option[AggregatedResult]
      ): Behavior[Command] = {
        Behaviors.receiveMessage {
          case GetResult(replyTo) =>
            maybeResult match {
              case Some(result) =>
                replyTo ! Result(result)
                Behaviors.stopped
              case None =>
                context.self ! GetResult(replyTo)
                Behaviors.same
            }

          case SendResult(result) =>
            manageResult(Some(result))
        }
      }
      manageResult(None)
    }
  }

}

object Leader {

  sealed trait Command
  case class RequestTask(giveWorkTo: ActorRef[DoTask]) extends Command

  def apply(tasks: Seq[Task]): Behavior[RequestTask] = {
    Behaviors.setup { context =>
      context.log.info("Starting Leader")
      val numberedTasks = tasks.zipWithIndex.map { case (task, taskNumber) =>
        DoTask(task, taskNumber)
      }

      def emitTasks(remainingTasks: Seq[DoTask]): Behavior[RequestTask] = {
        if (remainingTasks.isEmpty) Behaviors.stopped
        else {
          Behaviors.receiveMessage { case RequestTask(worker) =>
            worker ! remainingTasks.head
            emitTasks(remainingTasks.tail)
          }
        }
      }
      emitTasks(numberedTasks)
    }

  }
}

object Worker {
  sealed trait Command
  sealed trait UpdateResult
  case class DoTask(task: Task, taskNumber: Int) extends Command
  final case class UpdateSuccess(taskNumber: Int) extends UpdateResult
  final case class UpdateFailure(taskNumber: Int) extends UpdateResult
  final case class UpdateTimeout(taskNumber: Int) extends UpdateResult
  private final case class WrappedUpdateResult(result: UpdateResult)
      extends Command

  def apply(
      leader: ActorRef[Leader.RequestTask],
      aggregator: ActorRef[Aggregator.TaskResultMessage],
      timeout: Timeout
  ): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("Starting worker")
    leader ! RequestTask(context.self)

    // To timeout infinite futures
    def wrapTaskWithTimeout(task: Task): Future[Unit] = {
      val timeoutFuture = akka.pattern.after(timeout.duration)(
        Future.failed(new TimeoutException)
      )(context.system)
      Future.firstCompletedOf(Seq(task.execute, timeoutFuture))(
        context.system.executionContext
      )
    }

    def next(): Behavior[Command] = {
      Behaviors.receiveMessage {
        case DoTask(task, taskNumber) => {
          context.log.info(s"received task $task")
          val wrappedTaskFuture = wrapTaskWithTimeout(task)
          context.pipeToSelf(wrappedTaskFuture) {
            case Success(_) => WrappedUpdateResult(UpdateSuccess(taskNumber))
            case Failure(e) => {
              // pattern matching not working here, type erasure?
              if (e.isInstanceOf[TimeoutException])
                WrappedUpdateResult(UpdateTimeout(taskNumber))
              else WrappedUpdateResult(UpdateFailure(taskNumber))
            }
          }
          next()
        }
        case WrappedUpdateResult(result) => {
          context.log.info(s"task piped to self. Result: $result")
          val x = result match {
            case UpdateSuccess(taskNumber) => SuccessfulResult(taskNumber)
            case UpdateFailure(taskNumber) => FailedResult(taskNumber)
            case UpdateTimeout(taskNumber) => FailedResult(taskNumber)
          }
          aggregator ! Aggregator.TaskResultMessage(x)
          leader ! RequestTask(context.self)
          next()
        }
      }
    }
    next()
  }
}

case class AggregatedResult(
    successful: Seq[Int],
    failed: Seq[Int],
    timedOut: Seq[Int]
)

object Aggregator {
  sealed trait Command
  private case object ReceiveTimeout extends Command
  case class TaskResultMessage(result: TaskResult) extends Command
  case class GetAggregatedResults(replyTo: ActorRef[AggregatedResults])
      extends Command

  case class AggregatedResults(aggregatedResults: AggregatedResults)

  def apply(
      mainActor: ActorRef[MainActor.Command],
      timeout: FiniteDuration,
      expectedNumberOfTasks: Int
  ): Behavior[Command] = {
    Behaviors.setup { context =>
      context.log.info("Starting Aggregator")

      // Schedule timeout message
      context.system.scheduler.scheduleOnce(
        timeout,
        { () =>
          context.self ! ReceiveTimeout
        }
      )(context.system.executionContext)

      def aggregateResults(taskResults: Seq[TaskResult]): AggregatedResult = {
        val completedTasks = taskResults.map(_.taskNumber)
        val hungTasks =
          (0 until expectedNumberOfTasks)
            .filterNot(completedTasks.contains)
            .sorted

        AggregatedResult(
          taskResults
            .collect { case a: SuccessfulResult => a }
            .map(_.taskNumber),
          taskResults
            .collect { case a: FailedResult => a }
            .map(_.taskNumber),
          hungTasks
        )
      }

      def collectTaskResults(
          taskResults: immutable.IndexedSeq[TaskResult]
      ): Behavior[Command] = {
        Behaviors.receiveMessage {
          case TaskResultMessage(taskResult) =>
            context.log.info(s"received result $taskResult")
            val newTaskResults = taskResults :+ taskResult
            if (newTaskResults.size == expectedNumberOfTasks) {
              context.log.info(
                s"all $expectedNumberOfTasks completed, results: $newTaskResults"
              )
              val aggResults = aggregateResults(newTaskResults)
              context.log.info(s"returning results: $aggResults")
              mainActor ! SendResult(aggResults)
              Behaviors.stopped
            } else
              collectTaskResults(newTaskResults)

          case ReceiveTimeout =>
            context.log.info(s"received timeout, sending existing results")
            aggregateResults(taskResults)
            mainActor ! SendResult(aggregateResults(taskResults))
            Behaviors.stopped
        }
      }
      collectTaskResults(Vector.empty)
    }
  }
}
