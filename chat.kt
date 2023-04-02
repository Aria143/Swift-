import net.tomp2p.dht.FutureGet
import net.tomp2p.dht.FuturePut
import net.tomp2p.futures.BaseFutureAdapter
import net.tomp2p.futures.FutureBootstrap
import net.tomp2p.futures.FutureDiscover
import net.tomp2p.peers.Number160
import net.tomp2p.peers.PeerAddress
import net.tomp2p.peers.PeerMaker
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.net.InetAddress
import java.security.KeyPair
import java.security.KeyPairGenerator

class P2PNetwork(private val nodeId: String, private val bootstrapPeerIp: String) {

    private val keyPair: KeyPair = KeyPairGenerator.getInstance("DSA").genKeyPair()
    private val peer = PeerMaker(Number160.createHash(nodeId)).ports(4001).keyPair(keyPair).makeAndListenDHT()
    private val bootstrapPeer: PeerAddress = PeerAddress(Number160.ZERO, InetAddress.getByName(bootstrapPeerIp), 4001, 4001)

    init {
        connectToBootstrap()
    }

    private fun connectToBootstrap() {
        val futureDiscover: FutureDiscover = peer.peer().discover().inetAddress(InetAddress.getByName(bootstrapPeerIp)).ports(4001).start()
        futureDiscover.awaitUninterruptibly()

        if (!futureDiscover.isSuccess) {
            throw Exception("Failed to discover bootstrap peer. Reason: ${futureDiscover.failedReason()}")
        }

        val bootstrapAddress: PeerAddress = futureDiscover.peerAddress()
        val futureBootstrap: FutureBootstrap = peer.peer().bootstrap().addAddress(bootstrapAddress).start()
        futureBootstrap.awaitUninterruptibly()

        if (!futureBootstrap.isSuccess) {
            throw Exception("Failed to bootstrap to peer. Reason: ${futureBootstrap.failedReason()}")
        }
    }

    fun putFileChunks(key: String, file: File) {
        val chunkSize = 1024 * 1024 // 1MB chunk size
        var index = 0

        BufferedInputStream(FileInputStream(file)).use { input ->
            val buffer = ByteArray(chunkSize)

            while (input.read(buffer, 0, chunkSize).also { index = it } != -1) {
                val chunkKey = "${key}_$index"
                val futurePut: FuturePut = peer.put(Number160.createHash(chunkKey)).data(buffer.copyOf(index)).start()
                futurePut.awaitUninterruptibly()

                if (!futurePut.isSuccess) {
                    throw Exception("Failed to put chunk $chunkKey. Reason: ${futurePut.failedReason()}")
                }

                println("Chunk $chunkKey stored successfully")
            }
        }
    }

    fun getFile(key: String): ByteArray? {
        val futureGet: FutureGet = peer.get(Number160.createHash(key)).start()
        futureGet.awaitUninterruptibly()

        if (!futureGet.isSuccess) {
            throw Exception("Failed to get file. Reason: ${futureGet.failedReason()}")
        }

return if (futureGet.data().isEmpty) null else futureGet.data().values().iterator().next().data()
}

