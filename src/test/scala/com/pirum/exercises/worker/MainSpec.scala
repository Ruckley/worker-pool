package com.pirum.exercises.worker

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.concurrent.duration.DurationInt

class MainSpec extends AnyFlatSpec with Matchers {

  "runTasks" should "return correct tasks concurrently when no tasks hang" in {
    val tasks = List(SuccessTask(1), SuccessTask(2), FailureTask(1))

    Main.runTasks(tasks, 2.second, 3) shouldBe AggregatedResult(
      Vector(0, 1),
      Vector(2),
      Vector()
    )
  }

  "runTasks" should "return correct tasks concurrently when tasks hang" in {
    val tasks = List(SimulatedHangTask(1), SuccessTask(1), FailureTask(1))

    Main.runTasks(tasks, 1.second, 3) shouldBe AggregatedResult(
      Vector(1),
      Vector(2),
      Vector(0)
    )
  }

  "runTasks" should "return tasks in correct order when no tasks hang" in {
    val tasks = List(SuccessTask(3), SuccessTask(2), SuccessTask(1))

    Main.runTasks(tasks, 3.second, 3) shouldBe AggregatedResult(
      Vector(2, 1, 0),
      Vector(),
      Vector()
    )
  }

  "runTasks" should "return tasks in correct order when tasks hang" in {
    val tasks = List(SimulatedHangTask(3), SuccessTask(2), SuccessTask(1))

    Main.runTasks(tasks, 3.second, 3) shouldBe AggregatedResult(
      Vector(2, 1),
      Vector(),
      Vector(0)
    )
  }

  "runTasks" should "return tasks in correct order when less workers than tasks and no hangs" in {
    val tasks =
      List(SuccessTask(3), SuccessTask(2), SuccessTask(2), SuccessTask(0))

    Main.runTasks(tasks, 4.second, 2) shouldBe AggregatedResult(
      Vector(1, 0, 3, 2),
      Vector(),
      Vector()
    )
  }

  "runTasks" should "return tasks in correct order when less workers than tasks and worker occupied with hanging task" in {
    val tasks =
      List(SimulatedHangTask(4), SuccessTask(2), SuccessTask(3), SuccessTask(2))

    Main.runTasks(tasks, 4.second, 3) shouldBe AggregatedResult(
      Vector(1, 2, 3),
      Vector(),
      Vector(0)
    )
  }

  "runTasks" should "timeout successful and failed tasks if hangs occupy all workers" in {
    val tasks =
      List(
        SimulatedHangTask(1),
        SimulatedHangTask(1),
        SuccessTask(0),
        FailureTask(0)
      )

    Main.runTasks(tasks, 1.second, 2) shouldBe AggregatedResult(
      Vector(),
      Vector(),
      Vector(0, 1, 2, 3)
    )
  }

  "runTasks" should "handle stress test" in {
    val tasks = (0 until 65).map(_ => SuccessTaskMillis(100)).toList
    Main
      .runTasks(tasks, 5.second, 4)
      .successful
      .sorted shouldBe (0 until 65).toVector
  }

}
