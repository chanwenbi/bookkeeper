package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.jboss.netty.channel.Channel;

public abstract class PacketProcessorBaseV3 {

    final Request request;
    final Channel channel;
    final Bookie  bookie;

    public PacketProcessorBaseV3(Request request, Channel channel, Bookie bookie) {
        this.request = request;
        this.channel = channel;
        this.bookie = bookie;
    }

    protected boolean isVersionCompatible() {
        return this.request.getHeader().getVersion().equals(ProtocolVersion.VERSION_THREE);
    }

    /**
     * Build a header with protocol version 3 and the operation type same as what was in the
     * request.
     * @return
     */
    protected BKPacketHeader getHeader() {
        BKPacketHeader.Builder header = BKPacketHeader.newBuilder();
        header.setVersion(ProtocolVersion.VERSION_THREE);
        header.setOperation(request.getHeader().getOperation());
        header.setTxnId(request.getHeader().getTxnId());
        return header.build();
    }

}
