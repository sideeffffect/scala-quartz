package com.github.sideeffffect.quartz

import cats.effect.std.Dispatcher
import cats.effect.syntax.all._
import cats.effect.{Async, Resource, Sync}
import cats.syntax.all._
import com.github.sideeffffect.quartz.SchedulerQuartz._
import doobie.implicits._
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import io.circe.jawn.decode
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.quartz.spi.TriggerFiredBundle
import org.quartz.utils._

import java.sql.Connection
import java.time.Instant
import java.util.Properties
import javax.sql.DataSource
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.chaining._

private class SchedulerQuartz[A: Encoder: Decoder, F[_]: Sync](
    underlying: org.quartz.Scheduler,
    action: A => F[Unit],
    dispatcher: Dispatcher[F],
) extends com.github.sideeffffect.quartz.Scheduler[A, F]
    with Job {

  override def execute(context: JobExecutionContext): Unit = dispatcher.unsafeRunSync {
    for {
      jobData <- decode[A](context.getJobDetail.getJobDataMap.getString(jobDataMapKey)).liftTo[F]
      _ <- action(jobData)
    } yield ()
  }

  override def scheduleJob(name: String, group: String, jobData: A, cronExpression: String): F[Unit] = scheduleJob(
    name,
    group,
    jobData,
    _.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression).withMisfireHandlingInstructionFireAndProceed),
  ).void

  override def scheduleJob(name: String, group: String, jobData: A, instant: Instant): F[Unit] = scheduleJob(
    name,
    group,
    jobData,
    _.startAt(java.util.Date.from(instant))
      .withSchedule(SimpleScheduleBuilder.simpleSchedule.withMisfireHandlingInstructionFireNow),
  ).void

  def scheduleJob(
      name: String,
      group: String,
      jobData: A,
      configure: TriggerBuilder[Trigger] => TriggerBuilder[? <: Trigger],
  ): F[java.util.Date] = Sync[F].blocking {
    val jobKey = JobKey.jobKey(name, group)
    val jobDetail = JobBuilder
      .newJob(classOf[SchedulerQuartz[A, F]])
      .withIdentity(jobKey)
      .usingJobData(jobDataMapKey, jobData.asJson.spaces2SortKeys)
      .requestRecovery(true)
      .build()
    val trigger = TriggerBuilder
      .newTrigger()
      .withIdentity(name, group)
      .forJob(jobDetail)
      .pipe(configure)
      .build()
    if (underlying.checkExists(jobKey)) {
      val _ = underlying.deleteJob(jobKey)
    }
    underlying.scheduleJob(jobDetail, trigger)
  }

  override def checkExists(name: String, group: String): F[Boolean] = Sync[F].blocking {
    underlying.checkExists(JobKey.jobKey(name, group))
  }

  override def deleteJob(name: String, group: String): F[Boolean] = Sync[F].blocking {
    underlying.deleteJob(JobKey.jobKey(name, group))
  }
}

object SchedulerQuartz {

  private val jobDataMapKey = "jobDataJson"

  private val dataSourceKey = "org.quartz.jobStore.dataSource"
  private val instanceNameKey = "org.quartz.scheduler.instanceName"

  private def defaultQuartzConfig = Map(
    "org.quartz.scheduler.instanceName" -> "SchedulerQuartz",
    "org.quartz.scheduler.instanceId" -> "AUTO",
    //    "org.quartz.scheduler.wrapJobExecutionInUserTransaction" -> "false",

    // ThreadPool Configuration
    "org.quartz.threadPool.class" -> "org.quartz.simpl.SimpleThreadPool",
    "org.quartz.threadPool.threadCount" -> "10",
    "org.quartz.threadPool.threadPriority" -> Thread.NORM_PRIORITY.toString,

    // JobStore Configuration (JDBC for clustering)
    "org.quartz.jobStore.class" -> "org.quartz.impl.jdbcjobstore.JobStoreTX",
    "org.quartz.jobStore.driverDelegateClass" -> "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate",
    "org.quartz.jobStore.useProperties" -> "true", // Still useful for other JobStore props
    "org.quartz.jobStore.tablePrefix" -> "QRTZ_",

    // Clustering Configuration
    "org.quartz.jobStore.isClustered" -> "true",
    "org.quartz.jobStore.clusterCheckinInterval" -> "20000",
    "org.quartz.jobStore.misfireThreshold" -> "60000",
  )

  private def dbInit[F[_]: Sync](transactor: Transactor.Aux[F, DataSource])(dbInitScriptName: String): F[Unit] = for {
    dbInitScript <- Sync[F].blocking(
      getClass
        .getResourceAsStream(s"/org/quartz/impl/jdbcjobstore/$dbInitScriptName")
        .pipe(Source.fromInputStream)
        .mkString
        .pipe(Fragment.const(_)),
    )
    _ <- dbInitScript.updateWithLabel("SchedulerQuartzDbInit").run.transact(transactor)
  } yield ()

  def make[A: Encoder: Decoder, F[_]: Async](
      transactor: Transactor.Aux[F, DataSource],
      dbInitScriptName: Option[String] = None,
      customQuartzConfig: Map[String, String] = Map(),
  )(action: A => F[Unit]): Resource[F, com.github.sideeffffect.quartz.Scheduler[A, F]] = for {
    dispatcher <- Dispatcher.parallel[F](await = true)
    _ <- dbInitScriptName.traverse(dbInit(transactor)).toResource

    quartzConfig0 = defaultQuartzConfig ++ customQuartzConfig
    dataSourceValue = s"${quartzConfig0(instanceNameKey)}DS"
    quartzConfig = quartzConfig0 ++ Map(dataSourceKey -> dataSourceValue)

    scheduler <- Resource[F, SchedulerQuartz[A, F]](Sync[F].blocking {
      DBConnectionManager
        .getInstance()
        .addConnectionProvider(
          dataSourceValue, // Configure Quartz's JobStore to use the DataSource name we registered our provider under.
          new PoolingConnectionProvider {
            override def getDataSource: DataSource = transactor.kernel
            override def getConnection: Connection = transactor.kernel.getConnection
            override def shutdown(): Unit = ()
            override def initialize(): Unit = ()
          },
        )
      val props = { val p = new Properties(); p.putAll(quartzConfig.asJava); p }
      val schedulerFactory = new StdSchedulerFactory(props)
      val scheduler = schedulerFactory.getScheduler()
      val schedulerQuartz = new SchedulerQuartz[A, F](scheduler, action, dispatcher)
      scheduler.setJobFactory((_: TriggerFiredBundle, _: org.quartz.Scheduler) => schedulerQuartz)
      scheduler.start()
      (
        schedulerQuartz,
        Sync[F].blocking(scheduler.shutdown(true)),
      )
    })
  } yield scheduler

}
