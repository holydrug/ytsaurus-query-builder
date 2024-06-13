package io.github.holydrug.query

import io.github.holydrug.mapper.YtMapper
import io.github.holydrug.mapper.extension.getInstant
import io.github.holydrug.mapper.extension.getMapNullable
import io.github.holydrug.mapper.extension.getStringNullable
import io.github.holydrug.mapper.extension.toEpochMicro
import io.github.holydrug.scheme.YtCrudTable
import io.github.holydrug.scheme.toMap
import io.github.holydrug.scheme.toYTreeMap
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import tech.ytsaurus.client.DefaultSerializationResolver
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.typeinfo.TiType.*
import tech.ytsaurus.ysontree.YTreeMapNode
import java.time.Instant
import java.util.*

internal class YtCrudTableTest {
    data class DocError(
        // key
        val docId: UUID, val attribute: String, val index: Int,
        // value
        val code: String, val ts: Instant, val tags: Map<String, String>, val someNullable: String? = null
    ) {

        companion object {

            const val DOC_ID = "doc_id"
            const val ATTRIBUTE = "attribute"
            const val INDEX = "index"
            const val CODE = "code"
            const val TIMESTAMP = "timestamp"
            const val TAGS = "tags"
            const val SOME_NULLABLE = "some_nullable"
        }
    }


    private val errorYtMapper = object : YtMapper<DocError> {

        override fun toYtMap(r: DocError): Map<String, Any?> = mapOf(
            DocError.DOC_ID to r.docId.toString(),
            DocError.ATTRIBUTE to r.attribute,
            DocError.INDEX to r.index,
            DocError.CODE to r.code,
            DocError.TIMESTAMP to r.ts.toEpochMicro(),
            DocError.TAGS to r.tags.toYTreeMap(),
            DocError.SOME_NULLABLE to r.someNullable
        )

        override fun fromYt(r: YTreeMapNode): DocError = DocError(
            docId = r.getString(DocError.DOC_ID).let { UUID.fromString(it) },
            attribute = r.getString(DocError.ATTRIBUTE),
            index = r.getInt(DocError.INDEX),
            code = r.getString(DocError.CODE),
            ts = r.getInstant(DocError.TIMESTAMP, timestamp()),
            tags = r.getMapNullable(DocError.TAGS).toMap(),
            someNullable = r.getStringNullable(DocError.SOME_NULLABLE)
        )
    }

    private val errorSchema = YtCrudTable(
        YPath.simple("//home/doc").child("doc_error"),
        TableSchema.builder().setUniqueKeys(true).setStrict(true).addKey(DocError.DOC_ID, string())
            .addKey(DocError.ATTRIBUTE, string())
            .addKey(DocError.INDEX, int32()) // баг в MVP библиотеке - не умеет uint
            .addValue(DocError.CODE, string()).addValue(DocError.TIMESTAMP, timestamp())
            .addValue(DocError.TAGS, optional(yson())).addValue(DocError.SOME_NULLABLE, optional(string())).build(),
        errorYtMapper,
        "scan"
    )

    @Test
    fun insertManyTest() {
        errorSchema.insert(*(0 until 2).map { i ->
            DocError(
                docId = UUID.randomUUID(),
                attribute = "attr $i",
                index = i,
                code = "code $i",
                ts = Instant.now(),
                emptyMap()
            )
        }.toTypedArray()).also { it.convertValues(DefaultSerializationResolver.getInstance()) }.also {
            assertEquals(2, it.rows.size)
            assertEquals(7, it.rows[0].values.size) //7 schema columns noSkip
            assertEquals(null, it.rows[0].values[6].value) //nullable type no skip
        }
    }
}