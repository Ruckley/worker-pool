package com.pirum.exercises.worker

import scala.concurrent.Future

// A task that either succeeds after n seconds, fails after n seconds, or never terminates
sealed trait Task {
  def execute: Future[Unit]
}

// Purely for testing
case class SuccessTask(runTimeSeconds: Int) extends Task {
  override def execute: Future[Unit] = {
    Thread.sleep(runTimeSeconds * 1000)
    Future.successful()
  }
}

//for stress test
case class SuccessTaskMillis(runTimeMillis: Int) extends Task {
  override def execute: Future[Unit] = {
    Thread.sleep(runTimeMillis)
    Future.successful()
  }
}

case class FailureTask(runTimeSeconds: Int) extends Task {
  override def execute: Future[Unit] = {
    Thread.sleep(runTimeSeconds * 1000)
    Future.failed(new Exception)
  }
}

case class SimulatedHangTask(timeoutInSeconds: Int) extends Task {
  override def execute: Future[Unit] = {
    Thread.sleep((timeoutInSeconds + 1) * 1000)
    Future.failed(new Exception)
  }
}
