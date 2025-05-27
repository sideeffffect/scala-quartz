package com.github.sideeffffect.quartz

import java.time.Instant

trait Scheduler[A, F[_]] {
  def scheduleJob(name: String, group: String, jobData: A, cronExpression: String): F[Unit]
  def scheduleJob(name: String, group: String, jobData: A, instant: Instant): F[Unit]
  def checkExists(name: String, group: String): F[Boolean]
  def deleteJob(name: String, group: String): F[Boolean]
}
