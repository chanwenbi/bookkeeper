package org.apache.bookkeeper.proto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WriteEntryProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(WriteEntryProcessorV3.class);

    public WriteEntryProcessorV3(Request request, Channel channel, Bookie bookie) {
        super(request, channel, bookie);
    }

    // Returns null if there is no exception thrown
    private AddResponse getAddResponse() {
        AddRequest addRequest = request.getAddRequest();
        long ledgerId = addRequest.getLedgerId();
        long entryId = addRequest.getEntryId();

        final AddResponse.Builder addResponse = AddResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        if (!isVersionCompatible()) {
            addResponse.setStatus(StatusCode.EBADVERSION);
            return addResponse.build();
        }

        if (bookie.isReadOnly()) {
            logger.warn("BookieServer is running as readonly mode, so rejecting the request from the client!");
            addResponse.setStatus(StatusCode.EREADONLY);
            return addResponse.build();
        }

        BookkeeperInternalCallbacks.WriteCallback wcb = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId,
                                      InetSocketAddress addr, Object ctx) {
                Channel conn = (Channel) ctx;
                StatusCode status;
                switch (rc) {
                    case BookieProtocol.EOK:
                        status = StatusCode.EOK;
                        break;
                    case BookieProtocol.EIO:
                        status = StatusCode.EIO;
                        break;
                    default:
                        status = StatusCode.EUA;
                        break;
                }
                addResponse.setStatus(status);
                Response.Builder response = Response.newBuilder()
                        .setHeader(getHeader())
                        .setStatus(addResponse.getStatus())
                        .setAddResponse(addResponse);
                Response resp = response.build();
                conn.write(resp);
            }
        };
        StatusCode status = null;
        byte[] masterKey = addRequest.getMasterKey().toByteArray();
        ByteBuffer entryToAdd = addRequest.getBody().asReadOnlyByteBuffer();
        try {
            if (addRequest.hasFlag() && addRequest.getFlag().equals(AddRequest.Flag.RECOVERY_ADD)) {
                bookie.recoveryAddEntry(entryToAdd, wcb, channel, masterKey);
            } else {
                bookie.addEntry(entryToAdd, wcb, channel, masterKey);
            }
            status = StatusCode.EOK;
        } catch (IOException e) {
            logger.error("Error writing entry:{} to ledger:{}",
                         new Object[] { entryId, ledgerId, e });
            status = StatusCode.EIO;
        } catch (BookieException.LedgerFencedException e) {
            logger.debug("Ledger fenced while writing entry:{} to ledger:{}",
                         entryId, ledgerId);
            status = StatusCode.EFENCED;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:{} while writing entry:{}",
                         ledgerId, entryId);
            status = StatusCode.EUA;
        } catch (Throwable t) {
            logger.error("Unexpected exception while writing {}@{} : ",
                         new Object[] { entryId, ledgerId, t });
            // some bad request which cause unexpected exception
            status = StatusCode.EBADREQ;
        }

        // If everything is okay, we return null so that the calling function
        // doesn't return a response back to the caller.
        if (!status.equals(StatusCode.EOK)) {
            addResponse.setStatus(status);
            return addResponse.build();
        }
        return null;
    }

    @Override
    public void run() {
        AddResponse addResponse = getAddResponse();
        if (null != addResponse) {
            // This means there was an error and we should send this back.
            Response.Builder response = Response.newBuilder()
                    .setHeader(getHeader())
                    .setStatus(addResponse.getStatus())
                    .setAddResponse(addResponse);
            Response resp = response.build();
            channel.write(resp);
        }
    }
}
