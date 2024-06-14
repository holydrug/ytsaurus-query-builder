package io.github.holydrug

import io.github.holydrug.scheme.AbstractYtTable
import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.ApiServiceClient
import tech.ytsaurus.client.ApiServiceTransaction
import tech.ytsaurus.client.request.StartTransaction
import tech.ytsaurus.client.request.TransactionType
import tech.ytsaurus.ysontree.YTree
import java.util.concurrent.ConcurrentHashMap

suspend fun <T> ApiServiceClient.master(
  title: String,
  body: suspend (ApiServiceTransaction) -> T
): T = this.startTransaction(
  StartTransaction.builder()
    .setType(TransactionType.Master)
    .setSticky(false)
    .setPing(true)
    .setAttributes(mapOf("title" to YTree.stringNode(title)))
    .build()
).asyncAwait()!!.use { tx ->
  val rs = body(tx)
  tx.commit().asyncAwait()
  rs
}

/** Табличная транзакция (в контексте вызывающего) */
suspend fun <T> ApiServiceClient.tablet(
  body: suspend (ApiServiceTransaction) -> T
): T = try {
  this.startTransaction(
    StartTransaction.builder()
      .setType(TransactionType.Tablet)
      .setSticky(true)
      .setPing(true)
      .build()
  ).asyncAwait()!!.use { tx ->
    val rs = body(tx)
    tx.commit().asyncAwait()
    rs
  }
} catch (e: Throwable) {
  // TODO: не особо и нужно
//    if (false && e is YTsaurusError) {
//        val bodyName = body.javaClass.name.substringAfterLast('.')
//        val reduced = listOf(1702, 1703).map {
//            e.findMatchingError(it)
//        }.firstOrNull() ?: e.error
//        val merged = reduced.toBuilder().mergeAttributes(
//            TAttributeDictionary.newBuilder().addAttributes(
//                TAttribute.newBuilder().setKey("tx_name")
//                    .setValue(ByteString.copyFrom(YTree.stringNode(bodyName).toBinary())).build()
//            ).build()
//        ).build()
//        val ex = YTsaurusError(merged)
//        ex.stackTrace = emptyArray()
//        throw ex
//    }
  throw e
}

/** Средняя задержка потока сброса изменений на диск по всей таблице, миллисекунды */
suspend fun ApiServiceClient.flushLag(table: AbstractYtTable<*>) =
  getNode(table.path.attribute("flush_lag_time")).asyncAwait().longValue()

suspend fun ApiServiceClient.state(table: AbstractYtTable<*>): String =
  getNode(table.path.attribute("tablet_state")).asyncAwait().stringValue()

/** Задержка для согласования потока изменений с потоком сброса данных на диск */
suspend fun ApiServiceClient.delayByFlushLag(table: AbstractYtTable<*>) {
  // Если считать скорости записи на диск ~100МБ/с,
  // задержка связана с размером буферов в ОЗУ на всех табличных узлах
  // На LT01 доходит до ~970 при интенсивной записи.
  // Новые записи попадают как в pivot так и в Eden на узлы, иногда могут быть "перегретые" узлы
  val lag = flushLag(table) / 1000
  val oldLag = lags.put(table.name, lag)
  val millis = if (lag <= 10000) {
    lag / 100 // фактор смеси записи и сброса
  } else {
    100L // перераспределение разделов
  }
  if (lag != oldLag) {
    logger.info("table [{}] lag [{}] -> delay [{}] millis", table.simpleName, lag, millis)
  }
  delay(millis)
}


private val logger: Logger by lazy { LoggerFactory.getLogger(ApiServiceClient::class.java) }
private val lags = ConcurrentHashMap<String, Long>()
