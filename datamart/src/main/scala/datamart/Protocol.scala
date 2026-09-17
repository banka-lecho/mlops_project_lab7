package datamart

import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.parse
import io.circe.{Decoder, Json}

object Protocol {
  val RunCommands = Set("train", "predict")
  val FinishStatuses = Set("SUCCESS", "FAILED")

  final case class StartRunRequest(command: String, scalerPath: String, params: Json)

  final case class FinishRunRequest(
      runId: Int,
      status: String,
      rowsIn: Option[Int],
      bestK: Option[Int],
      bestSilhouette: Option[Double],
      params: Option[Json],
      errorMessage: Option[String],
      scalerPath: Option[String]
  )

  final case class TrainRunRequest(runId: Option[Int])

  final case class TrainRun(runId: Int, modelPath: String, scalerPath: String)

  implicit val startRunDecoder: Decoder[StartRunRequest] = deriveDecoder
  implicit val finishRunDecoder: Decoder[FinishRunRequest] = deriveDecoder
  implicit val trainRunDecoder: Decoder[TrainRunRequest] = deriveDecoder

  val Status = "status"
  val Data = "data"
  val Error = "error"

  val StatusOk = "ok"
  val StatusError = "error"

  def ok(data: Json): Json =
    Json.obj(Status -> Json.fromString(StatusOk), Data -> data)

  def error(message: String): Json =
    Json.obj(Status -> Json.fromString(StatusError), Error -> Json.fromString(message))

  final case class ProtocolError(httpCode: Int, message: String) extends RuntimeException(message)

  def badRequest(message: String): ProtocolError = ProtocolError(400, message)

  def decodeRequest[A: Decoder](body: String): A = {
    val json =
      if (body.trim.isEmpty) Json.obj()
      else parse(body).fold(e => throw badRequest(s"Тело запроса — не JSON: ${e.message}"), identity)
    json.as[A].fold(e => throw badRequest(s"Неверный формат запроса: ${e.getMessage}"), identity)
  }
}
