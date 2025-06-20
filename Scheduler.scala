package com.github.sideeffffect.quartz

import org.quartz.{CronExpression, JobDetail, JobKey, Trigger, TriggerBuilder}

import java.time.Instant

trait Scheduler[A, F[_]] {
  def scheduleJob(trigger: Trigger): F[Instant]
  def scheduleJob(jobDetail: JobDetail, trigger: Trigger): F[Instant]
  def checkExists(jobKey: JobKey): F[Boolean]
  def deleteJob(jobKey: JobKey): F[Boolean]
}

trait SchedulerCustom[A, F[_]] extends Scheduler[A, F] {
  def scheduleJobCustom(jobKey: JobKey, jobData: A, cronExpression: CronExpression): F[Unit]
  def scheduleJobCustom(jobKey: JobKey, jobData: A, instant: Instant): F[Unit]
  def scheduleJobCustom(
      jobKey: JobKey,
      jobData: A,
      configure: TriggerBuilder[Trigger] => TriggerBuilder[? <: Trigger],
  ): F[Instant]
}
