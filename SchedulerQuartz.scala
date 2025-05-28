package com.github.sideeffffect.quartz

import cats.MonadThrow
import cats.effect.std.Dispatcher
import cats.effect.syntax.all._
import cats.effect.{Async, MonadCancelThrow, Resource, Sync}
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

private class SchedulerQuartz[A: Encoder: Decoder, F[_]: MonadThrow, G[_]: Sync](
    underlying: org.quartz.Scheduler,
    action: A => F[Unit],
    dispatcher: Dispatcher[F],
) extends com.github.sideeffffect.quartz.Scheduler[A, G]
    with Job {

  def executeF(context: JobExecutionContext): F[Unit] = for {
    jobData <- decode[A](context.getJobDetail.getJobDataMap.getString(jobDataMapKey)).liftTo[F]
    _ <- action(jobData)
  } yield ()

  override def execute(context: JobExecutionContext): Unit = dispatcher.unsafeRunSync(executeF(context))

  override def scheduleJob(name: String, group: String, jobData: A, cronExpression: String): G[Unit] = scheduleJob(
    name,
    group,
    jobData,
    _.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression).withMisfireHandlingInstructionFireAndProceed),
  ).void

  override def scheduleJob(name: String, group: String, jobData: A, instant: Instant): G[Unit] = scheduleJob(
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
  ): G[java.util.Date] = Sync[G].blocking {
    val jobKey = JobKey.jobKey(name, group)
    val jobDetail = JobBuilder
      .newJob(classOf[SchedulerQuartz[A, F, G]])
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

  override def checkExists(name: String, group: String): G[Boolean] = Sync[G].blocking {
    underlying.checkExists(JobKey.jobKey(name, group))
  }

  override def deleteJob(name: String, group: String): G[Boolean] = Sync[G].blocking {
    underlying.deleteJob(JobKey.jobKey(name, group))
  }
}

object SchedulerQuartz {

  private val jobDataMapKey = "jobDataJson"

  private val dataSourceKey = "org.quartz.jobStore.dataSource"
  private val instanceNameKey = "org.quartz.scheduler.instanceName"
  private val tablePrefixKey = "org.quartz.jobStore.tablePrefix"

  private val defaultQuartzConfig = Map(
    instanceNameKey -> "SchedulerQuartz",
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
    tablePrefixKey -> "QRTZ_",

    // Clustering Configuration
    "org.quartz.jobStore.isClustered" -> "true",
    "org.quartz.jobStore.clusterCheckinInterval" -> "20000",
    "org.quartz.jobStore.misfireThreshold" -> "60000",
  )

  private def dbInit[F[_]: Sync, DS <: DataSource](
      transactor: Transactor.Aux[F, DS],
  )(dbInitScriptName: String): F[Unit] = for {
    dbInitScript <- Sync[F].blocking(
      getClass
        .getResourceAsStream(s"/org/quartz/impl/jdbcjobstore/$dbInitScriptName")
        .pipe(Source.fromInputStream)
        .mkString
        .pipe(Fragment.const(_)),
    )
    _ <- dbInitScript.updateWithLabel("SchedulerQuartzDbInit").run.transact(transactor)
  } yield ()

  private def isDbInitialized[F[_]: MonadCancelThrow, DS <: DataSource](
      transactor: Transactor.Aux[F, DS],
      prefix: String,
      schema: String = "public",
  ): F[Boolean] = sql"""
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = $schema AND table_name LIKE ${prefix + "%"}
    )
  """.query[Boolean].unique.transact(transactor)

  def make[A: Encoder: Decoder, DS <: DataSource, F[_]: Async, G[_]: Sync](
      transactor: Transactor.Aux[F, DS],
      dbInitScriptName: Option[String] = None,
      customQuartzConfig: Map[String, String] = Map(),
  )(action: A => F[Unit]): Resource[F, com.github.sideeffffect.quartz.Scheduler[A, G]] = for {
    dispatcher <- Dispatcher.parallel[F](await = true)

    quartzConfig0 = defaultQuartzConfig ++ customQuartzConfig
    dataSourceValue = s"${quartzConfig0(instanceNameKey)}DS"
    quartzConfig = quartzConfig0 ++ Map(dataSourceKey -> dataSourceValue)

    dbIsInitialized <- isDbInitialized(transactor, quartzConfig(tablePrefixKey)).toResource
    _ <- Async[F].unlessA(dbIsInitialized)(dbInitScriptName.traverse(dbInit(transactor))).toResource

    scheduler <- Resource[F, SchedulerQuartz[A, F, G]](Sync[F].blocking {
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
      val schedulerQuartz = new SchedulerQuartz[A, F, G](scheduler, action, dispatcher)
      scheduler.setJobFactory((_: TriggerFiredBundle, _: org.quartz.Scheduler) => schedulerQuartz)
      scheduler.start()
      (
        schedulerQuartz,
        Sync[F].blocking(scheduler.shutdown(true)),
      )
    })
  } yield scheduler

}
