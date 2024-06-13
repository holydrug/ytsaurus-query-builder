package io.github.holydrug.mapper.extension

import java.time.Instant
import java.time.temporal.ChronoUnit

private const val MICROSECONDS_PER_SECOND = 1_000_000
private const val NANOS_PER_MICROSECOND = 1_000

/**
 * Переводим секунды и наносекунды временной метки в микросекунды.
 * Подробнее смотрите в [документации](https://ytsaurus.tech/docs/ru/yql/types/primitive#datetime:~:text=Timestamp,%D1%82%D0%BE%D1%87%D0%BD%D0%BE%D1%81%D1%82%D1%8C%20%D0%B4%D0%BE%20%D0%BC%D0%B8%D0%BA%D1%80%D0%BE%D1%81%D0%B5%D0%BA%D1%83%D0%BD%D0%B4).
 * @return Количество микросекунд с unix эпохи.
 */
fun Instant.toEpochMicro() = epochSecond * MICROSECONDS_PER_SECOND + nano / NANOS_PER_MICROSECOND

/**
 * Создает Instant из количества микросекунд с эпохи, с учетом указанной единицы измерения времени.
 *
 * @param chronicUnit Единица измерения времени для округления результата.
 * @return [Instant], представляющий указанный момент времени.
 */
fun Long.toInstantFromMicro(chronicUnit: ChronoUnit): Instant =
    Instant.ofEpochSecond(
        this / MICROSECONDS_PER_SECOND,
        (this % MICROSECONDS_PER_SECOND) * NANOS_PER_MICROSECOND
    ).truncatedTo(chronicUnit)