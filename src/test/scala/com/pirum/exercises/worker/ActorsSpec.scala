package com.pirum.exercises.worker

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import com.pirum.exercises.worker.Aggregator.TaskResultMessage
import com.pirum.exercises.worker.Leader.RequestTask
import com.pirum.exercises.worker.Worker.DoTask
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class ActorsSpec extends AnyWordSpec with BeforeAndAfterAll with Matchers {

  private val testKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  private val hangTimeout = 4
  private val timeout = FiniteDuration(1, "second")

  val (aggProbe, dummyAggregator) = {
    val aggMockedBehavior =
      Behaviors.receiveMessage[Aggregator.TaskResultMessage] { request =>
        Behaviors.same
      }
    val aggProbe = testKit.createTestProbe[Aggregator.TaskResultMessage]
    val dummyAggregator =
      testKit.spawn(Behaviors.monitor(aggProbe.ref, aggMockedBehavior))
    (aggProbe, dummyAggregator)
  }

  val (workerProbe, dummyWorker) = {
    val workerMockedBehavior =
      Behaviors.receiveMessage[Worker.DoTask] { request => Behaviors.same }
    val workerProbe = testKit.createTestProbe[Worker.DoTask]
    val dummyWorker =
      testKit.spawn(Behaviors.monitor(workerProbe.ref, workerMockedBehavior))
    (workerProbe, dummyWorker)
  }

  val (leaderProbe, dummyLeader) = {
    val leaderMockedBehavior =
      Behaviors.receiveMessage[Leader.RequestTask] { request => Behaviors.same }
    val leaderProbe = testKit.createTestProbe[Leader.RequestTask]
    val dummyLeader =
      testKit.spawn(Behaviors.monitor(leaderProbe.ref, leaderMockedBehavior))
    (leaderProbe, dummyLeader)
  }

  val (mainActorProbe, dummyMainActor) = {
    val mainActorMockedBehavior =
      Behaviors.receiveMessage[MainActor.Command] { request => Behaviors.same }
    val mainActorProbe = testKit.createTestProbe[MainActor.Command]
    val dummyLeader =
      testKit.spawn(
        Behaviors.monitor(mainActorProbe.ref, mainActorMockedBehavior)
      )
    (mainActorProbe, dummyLeader)
  }

  "leader" should {
    "respond to RequestWork with next task" in {

      val tasks =
        List(FailureTask(0), SuccessTask(0), SimulatedHangTask(5))

      val testLeader = testKit.spawn(Leader(tasks), "leader")

      tasks.zipWithIndex.map { case (task, index) =>
        testLeader ! RequestTask(dummyWorker)
        workerProbe.expectMessage(DoTask(task, index))
      }

    }
  }

  "worker" should {
    "request task on setup" in {
      val worker =
        testKit.spawn(Worker(dummyLeader, dummyAggregator, timeout))
      leaderProbe.expectMessage(RequestTask(worker))
    }

    "perform task on DoTask" in {
      val worker =
        testKit.spawn(Worker(dummyLeader, dummyAggregator, timeout))

      leaderProbe.expectMessage(Leader.RequestTask(worker))

      worker ! DoTask(SuccessTask(0), 1)

      aggProbe.expectMessage(Aggregator.TaskResultMessage(SuccessfulResult(1)))
      leaderProbe.expectMessage(Leader.RequestTask(worker))
    }

    "perform multiple tasks sequentially" in {
      val testWorker =
        testKit.spawn(Worker(dummyLeader, dummyAggregator, timeout))

      leaderProbe.expectMessage(Leader.RequestTask(testWorker))

      testWorker ! DoTask(SuccessTask(0), 0)
      testWorker ! DoTask(FailureTask(0), 1)

      leaderProbe.expectMessage(Leader.RequestTask(testWorker))
      leaderProbe.expectMessage(Leader.RequestTask(testWorker))
      leaderProbe.expectNoMessage()

      aggProbe.expectMessage(Aggregator.TaskResultMessage(SuccessfulResult(0)))
      aggProbe.expectMessage(Aggregator.TaskResultMessage(FailedResult(1)))
      aggProbe.expectNoMessage()

    }

    "continue to process tasks if another worker is hung" in {
      val testWorker =
        testKit.spawn(Worker(dummyLeader, dummyAggregator, timeout))
      leaderProbe.expectMessage(Leader.RequestTask(testWorker))
      val hungTestWorker =
        testKit.spawn(Worker(dummyLeader, dummyAggregator, timeout))
      leaderProbe.expectMessage(Leader.RequestTask(hungTestWorker))

      hungTestWorker ! DoTask(SimulatedHangTask(hangTimeout), 1)
      testWorker ! DoTask(SuccessTask(0), 2)

      leaderProbe.expectMessage(Leader.RequestTask(testWorker))
      leaderProbe.expectNoMessage()

      aggProbe.expectMessage(Aggregator.TaskResultMessage(SuccessfulResult(2)))
      aggProbe.expectNoMessage()
    }
  }

  "aggregator" should {
    "aggregate when all tasks finish and send to mainActor" in {

      val taskResults =
        List(SuccessfulResult(0), FailedResult(1), SuccessfulResult(2))

      val testAggregator =
        testKit.spawn(Aggregator(dummyMainActor, 5.seconds, taskResults.size))

      taskResults.foreach(res => testAggregator ! TaskResultMessage(res))

      mainActorProbe.expectMessage(
        MainActor.SendResult(
          AggregatedResult(Vector(0, 2), Vector(1), Vector())
        )
      )

    }

    "aggregate finished tasks after timeout and send to mainActor" in {

      val taskResults =
        List(SuccessfulResult(0), FailedResult(2), SuccessfulResult(3))

      val testAggregator =
        testKit.spawn(Aggregator(dummyMainActor, 1.seconds, 5))

      taskResults.foreach(res => testAggregator ! TaskResultMessage(res))

      mainActorProbe.expectMessage(
        MainActor.SendResult(
          AggregatedResult(
            successful = Vector(0, 3),
            failed = Vector(2),
            timedOut = Vector(1, 4)
          )
        )
      )

    }
  }
}
