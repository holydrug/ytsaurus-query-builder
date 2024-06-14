package io.github.holydrug

import kotlinx.coroutines.CompletableDeferred
import tech.ytsaurus.TError
import tech.ytsaurus.core.common.YTsaurusError
import java.net.ConnectException
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeoutException

/** Асинхронный фиксатор результата исполнения */
fun <T> CompletionStage<T>.asAsyncDeferred(): CompletableDeferred<T> {
    val deferred = CompletableDeferred<T>()
    this.whenCompleteAsync { value, exception ->
        if (exception == null) {
            deferred.complete(value)
        } else {
            val cause = when (exception) {
                is CompletionException, is ExecutionException -> exception.cause ?: exception
                else -> exception
            }
            deferred.completeExceptionally(cause)
        }
    }
    if (this is Future<*>) {
        deferred.invokeOnCompletion { this.cancel(false) }
    }
    return deferred
}

/** Дождаться и сместить стек */
suspend fun <T> CompletionStage<T>.asyncAwait(): T = try {
    asyncAwait0()
} catch (e: ConnectException) {
    throw ConnectException(e.message).also { cleanupContinuationStack(it) }
} catch (e: TimeoutException) {
    throw TimeoutException(e.message).also { cleanupContinuationStack(it) }
} catch (e: YTsaurusError) {
    throw YTsaurusError(rootError(e.error)).also { cleanupContinuationStack(it) }
}

suspend inline fun <T> CompletionStage<T>.asyncAwait0() = asAsyncDeferred().await()

/**
 * Найти корень проблемы
 */
fun rootError(error: TError): TError {
    var cause = error
    while (true) {
        val inner = cause.innerErrorsList
        if (inner.size == 1) {
            cause = inner.first()
        } else {
            break
        }
    }
    return cause
}

/**
 * Очистить стек сопрограммы до точки продолжения сверху и снизу
 */
private fun cleanupContinuationStack(e: Throwable) {
    val index = e.stackTrace.indexOfLast {
        "kotlin.coroutines.jvm.internal.BaseContinuationImpl" == it.className
    }
    if (index > 1) {
        val start = if (e.stackTrace[0].className.startsWith("pro.ittds.yt.YtDeferredKt")) 1 else 0
        e.stackTrace = e.stackTrace.copyOfRange(start, index)
    }
}

