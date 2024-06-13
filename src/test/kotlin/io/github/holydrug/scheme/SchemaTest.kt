package io.github.holydrug.scheme

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import tech.ytsaurus.client.request.SerializationContext
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Table

internal class SchemaTest {

    /** YT Java API умеет работать лишь с внедрением простых свойств  */
    @Entity
    @Table(name = "cis_order")
    class CisOrderBuilder {
        @Column(name = "device_id", nullable = false)
        var deviceId: String = ""
        @Column(name = "order_id", nullable = false)
        var orderId: Long = 0
        @Column(nullable = false)
        var requestTime: Long = 0
        @Column(nullable = false)
        var status: Byte = 0
        var statusDescription: String? = null
        @Column(nullable = false)
        var codesCount: Int = 0
        var lastRequestedCodeIndex: Int? = null
        var lastRequestedQuantity: Int? = null
        @Column(nullable = false)
        var codeTemplate: String = ""
        @Column(nullable = false)
        var codeType: Short = 0
        @Column(nullable = false)
        var serialNumberType: Byte = 0
        @Column(nullable = false)
        var tags: Map<String, String> = emptyMap()
        @Column(nullable = false)
        var productGroup: Int = 0
        @Column(nullable = false)
        var participantId: String = ""
        @Column(nullable = false)
        var businessId: String = ""
        @Column(nullable = false)
        var ownerInn: String = ""
        @Column(nullable = false)
        var version: Int = 0
        var insertedBy: String? = null
    }

    @Test
    fun serializationContext() {
        val objectClass = CisOrderBuilder::class.java
        val context = SerializationContext(objectClass)
        assertEquals(objectClass, context.objectClass.get())
        val format = context.format.get()
        assertEquals("skiff", format.type)
        assertEquals(setOf("skiff_schema_registry", "table_skiff_schemas"), format.attributes.keys)
        assertEquals("[\"\$table\";]", format.attributes["table_skiff_schemas"].toString())
        val skiffSerializer = context.skiffSerializer.get()
        val objectA = CisOrderBuilder().apply {
            deviceId = "deviceId"
            orderId = 1L
            status = 1
            codesCount = 1
            participantId = "123"
            businessId = "123"
            version = 1
            ownerInn = "123465"
            productGroup = 1
        }
        val serializedA = skiffSerializer.serialize(objectA)
        assertEquals(77, serializedA.size)
        val objectB = skiffSerializer.deserialize(serializedA).get()
        val serializedB = skiffSerializer.serialize(objectB)
        assertArrayEquals(serializedA, serializedB)
    }
}