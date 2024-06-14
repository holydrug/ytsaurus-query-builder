package io.github.holydrug

import io.github.holydrug.config.YtClientProperties
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.DirectYTsaurusClient
import tech.ytsaurus.client.rpc.YTsaurusClientAuth
import java.net.InetSocketAddress

object YtClientFactory {

  fun direct(props: YtClientProperties): CompoundClient {
    val parts = props.cluster.split(':')
    val host = parts[0]
    val port = parts[1].toInt()
    return DirectYTsaurusClient.builder()
      .setAddress(InetSocketAddress.createUnresolved(host, port))
      .setAuth(YTsaurusClientAuth.builder().setToken(props.token).setUser(props.user).build())
      .build()
  }
}