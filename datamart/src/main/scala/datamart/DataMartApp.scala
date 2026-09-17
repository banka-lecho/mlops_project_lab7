package datamart

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.circe.Json
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, Executors}
import scala.util.control.NonFatal

import Protocol._

object DataMartApp {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = MartConfig.load()
    println(s"Источник данных: ${config.datasource}")

    val spark = SparkSession
      .builder()
      .appName(config.spark.appName)
      .master(config.spark.master)
      .config("spark.sql.shuffle.partitions", config.spark.shufflePartitions.toString)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel(config.spark.logLevel)

    val server = HttpServer.create(new InetSocketAddress(config.server.host, config.server.port), 0)
    server.setExecutor(Executors.newFixedThreadPool(config.server.threads))

    route(server, "GET", "/health") { _ =>
      Json.obj(
        "sparkVersion" -> Json.fromString(spark.version),
        "database" -> Json.fromString(config.datasource.database)
      )
    }

    val store = new MsSqlStore(config.datasource)

    route(server, "POST", "/v1/runs/start") { body =>
      val req = decodeRequest[StartRunRequest](body)
      if (!RunCommands.contains(req.command))
        throw badRequest(s"command должен быть одним из ${RunCommands.mkString(", ")}")
      Json.obj("runId" -> Json.fromInt(store.startRun(req)))
    }

    route(server, "POST", "/v1/runs/finish") { body =>
      val req = decodeRequest[FinishRunRequest](body)
      if (!FinishStatuses.contains(req.status))
        throw badRequest(s"status должен быть одним из ${FinishStatuses.mkString(", ")}")
      if (!store.finishRun(req)) throw ProtocolError(404, s"Запуск run_id=${req.runId} не найден")
      Json.obj("runId" -> Json.fromInt(req.runId))
    }

    route(server, "POST", "/v1/runs/train") { body =>
      val req = decodeRequest[TrainRunRequest](body)
      val run = store.getTrainRun(req.runId).getOrElse {
        val which = req.runId.fold("ни одного")(id => s"run_id=$id")
        throw ProtocolError(404, s"Не найден успешный запуск обучения с сохранённой моделью ($which)")
      }
      Json.obj(
        "runId" -> Json.fromInt(run.runId),
        "modelPath" -> Json.fromString(run.modelPath),
        "scalerPath" -> Json.fromString(run.scalerPath)
      )
    }

    server.createContext(
      "/",
      (exchange: HttpExchange) =>
        try write(exchange, 404, error(s"Нет маршрута ${exchange.getRequestURI.getPath}"))
        finally exchange.close()
    )

    val stopped = new CountDownLatch(1)
    sys.addShutdownHook {
      println("Останавливаю витрину")
      server.stop(0)
      spark.stop()
      stopped.countDown()
    }

    server.start()
    println(s"Витрина слушает ${config.server.host}:${config.server.port}")
    stopped.await()
  }

  private def route(server: HttpServer, method: String, path: String)(action: String => Json): Unit =
    server.createContext(
      path,
      (exchange: HttpExchange) =>
        try {
          if (exchange.getRequestURI.getPath != path) {
            write(exchange, 404, error(s"Нет маршрута ${exchange.getRequestURI.getPath}"))
          } else if (exchange.getRequestMethod != method) {
            write(exchange, 405, error(s"Ожидается $method"))
          } else {
            val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
            write(exchange, 200, ok(action(body)))
          }
        } catch {
          case e: ProtocolError =>
            write(exchange, e.httpCode, error(e.message))
          case NonFatal(e) =>
            log.error(s"$method $path упал", e)
            write(exchange, 500, error(s"${e.getClass.getSimpleName}: ${e.getMessage}"))
        } finally exchange.close()
    )

  private def write(exchange: HttpExchange, code: Int, json: Json): Unit = {
    val bytes = json.noSpaces.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(code, bytes.length.toLong)
    exchange.getResponseBody.write(bytes)
  }
}
