package de.gsi.microservice.datasource.cmwlight;

import de.gsi.microservice.EventStore;
import de.gsi.microservice.Filter;
import de.gsi.microservice.datasource.DataSource;
import de.gsi.microservice.utils.SharedPointer;
import de.gsi.serializer.IoSerialiser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A lightweight implementation of the CMW RDA client part.
 * Reads all sockets from a single Thread, which can also be embedded into other event loops.
 * Manages connection state and automatically reconnects broken connections and subscriptions.
 */
public class CmwLightClient implements DataSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(CmwLightClient.class);
    private static final AtomicLong connectionIdGenerator = new AtomicLong(0); // global counter incremented for each connection
    private static final AtomicInteger requestIdGenerator = new AtomicInteger(0);
    protected final AtomicInteger channelId = new AtomicInteger(0); // connection local counter incremented for each channel
    protected final ZMQ.Context context;
    protected final ZMQ.Socket controlChannel;
    protected final AtomicReference<ConnectionState> connectionState = new AtomicReference<>(ConnectionState.DISCONNECTED);
    protected final String address;
    protected String sessionId;
    protected long connectionId;
    protected final Map<Long, Subscription> subscriptions = Collections.synchronizedMap(new HashMap<>()); // all subscriptions added to the server
    protected long lastHbReceived = -1;
    protected long lastHbSent = -1;
    protected static final int heartbeatInterval = 1000; // time between to heartbeats in ms
    protected static final int heartbeatAllowedMisses = 3; // number of heartbeats which can be missed before resetting the conection
    protected static final long subscriptionTimeout = 1000; // maximum time after which a connection should be reconnected
    protected int backOff = 20;

    public void unsubscribe(final Subscription subscription) throws CmwLightProtocol.RdaLightException {
        subscriptions.remove(subscription.id);
        sendUnsubscribe(subscription);
    }

    public static boolean matches(final String address) {
        return address.startsWith("tcp://") || address.startsWith("rda3://");
    }

    @Override
    public boolean poll(final EventStore eventStore) { // remove deadline and only process one at a time?
        CmwLightMessage reply = receiveData();
        if (reply == null) {
            return false;
        }
        if (reply.messageType == CmwLightProtocol.MessageType.SERVER_HB || reply.messageType == CmwLightProtocol.MessageType.SERVER_CONNECT_ACK) {
            return true;
        }
        eventStore.getRingBuffer().publishEvent(((event, sequence, arg0) -> {
            event.arrivalTimeStamp = System.currentTimeMillis();
            event.payload = new SharedPointer<>();
            event.payload.set(arg0);
            if (arg0.requestType.equals(CmwLightProtocol.RequestType.NOTIFICATION_DATA)) {
                final DataSourceFilter dataSourceFilter = event.getFilter(DataSourceFilter.class);
                dataSourceFilter.eventType = EventType.NOTIFICATION_UPDATE;
                dataSourceFilter.requestedDomainObjType = subscriptions.values().stream().filter(s -> s.updateId == arg0.id).map(s -> s.domainClass).findAny().orElse(null);
            } else if (arg0.requestType.equals(CmwLightProtocol.RequestType.REPLY)) {
                event.getFilter(DataSourceFilter.class).eventType = EventType.GET_REPLY;
            } else {
                event.getFilter(DataSourceFilter.class).eventType = EventType.STATE_CHANGE;
            }
        }), reply);
        return true;
    }

    public enum ConnectionState {
        DISCONNECTED,
        CONNECTING,
        CONNECTED
    }

    public CmwLightClient(String address, ZMQ.Context context) {
        LOGGER.atTrace().addArgument(address).log("connecting to: {}");
        this.context = context;
        controlChannel = context.socket(SocketType.DEALER);
        controlChannel.setIdentity(getIdentity().getBytes()); // hostname/process/id/channel
        controlChannel.setSndHWM(0);
        controlChannel.setRcvHWM(0);
        controlChannel.setLinger(0);
        this.address = address;
        this.sessionId = getSessionId();
    }

    @Override
    public String getAddress() {
        return address;
    }

    public ConnectionState getConnectionState() {
        return connectionState.get();
    }

    public ZMQ.Context getContext() {
        return context;
    }

    public ZMQ.Socket getSocket() {
        return controlChannel;
    }

    private String getIdentity() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "localhost";
        }
        final long processId = ProcessHandle.current().pid();
        connectionId = connectionIdGenerator.incrementAndGet();
        final int chId = this.channelId.incrementAndGet();
        return hostname + '/' + processId + '/' + connectionId + '/' + chId;
    }

    private String getSessionId() {
        return "asdf";
    }

    public void connect() {
        LOGGER.atDebug().addArgument(address).log("connecting to {}");
        if (connectionState.getAndSet(ConnectionState.CONNECTING) != ConnectionState.DISCONNECTED) {
            return;
        }
        controlChannel.connect(address);
        try {
            CmwLightProtocol.serialiseMsg(CmwLightMessage.connect("1.0.0")).send(controlChannel);
            connectionState.set(ConnectionState.CONNECTING);
            lastHbReceived = System.currentTimeMillis();
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("failed to connect: ");
            backOff = backOff * 2;
            resetConnection();
        }
    }

    private void resetConnection() {
        disconnect();
        connect();
    }

    private void disconnect() {
        LOGGER.atDebug().addArgument(address).log("disconnecting {}");
        connectionState.set(ConnectionState.DISCONNECTED);
        controlChannel.disconnect(address);
        // disconnect/reset subscriptions
    }

    public CmwLightMessage receiveData() {
        final long currentTime = System.currentTimeMillis();
        // receive data
        try {
            final ZMsg data = ZMsg.recvMsg(controlChannel, ZMQ.DONTWAIT);
            if (data == null)
                return null;
            final CmwLightMessage reply = CmwLightProtocol.parseMsg(data);
            if (connectionState.get().equals(ConnectionState.CONNECTING) && reply.messageType == CmwLightProtocol.MessageType.SERVER_CONNECT_ACK) {
                connectionState.set(ConnectionState.CONNECTED);
                backOff = 20; // reset back-off time
                return reply;
            }
            if (connectionState.get() != ConnectionState.CONNECTED) {
                LOGGER.atWarn().addArgument(reply).log("received data before connection established: {}");
            }
            if (reply.requestType == CmwLightProtocol.RequestType.SUBSCRIBE_EXCEPTION) {
                final long id = reply.id;
                final Subscription sub = subscriptions.get(id);
                sub.subscriptionState = SubscriptionState.UNSUBSCRIBED;
                sub.timeoutValue = currentTime + sub.backOff;
                sub.backOff *= 2;
                LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("exception during subscription, retrying: {}/{}");
            }
            if (reply.requestType == CmwLightProtocol.RequestType.SUBSCRIBE) {
                final long id = reply.id;
                final Subscription sub = subscriptions.get(id);
                sub.updateId = (long) reply.options.get(CmwLightProtocol.FieldName.SOURCE_ID_TAG.value());
                sub.subscriptionState = SubscriptionState.SUBSCRIBED;
                LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("subscription sucessful: {}/{}");
                sub.backOff = 20;
            }
            lastHbReceived = currentTime;
            return reply;
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("error parsing cmw light reply: ");
            return null;
        }
    }

    @Override
    public long doHousekeeping(final long currentTime) {
        switch (connectionState.get()) {
        case DISCONNECTED: // reconnect after adequate back off
            if (currentTime > lastHbSent + backOff) {
                connect();
            }
            return lastHbSent + backOff;
        case CONNECTING:
            if (currentTime > lastHbReceived + heartbeatInterval * heartbeatAllowedMisses) { // connect timed out -> increase back of and retry
                backOff = backOff * 2;
                resetConnection();
            }
            return lastHbReceived + heartbeatInterval * heartbeatAllowedMisses;
        case CONNECTED:
            if (currentTime > lastHbSent + heartbeatInterval) { // check for heartbeat interval
                // send Heartbeats
                sendHeartBeat();
                lastHbSent = currentTime;
                // check if heartbeat was received
                if (lastHbReceived + heartbeatInterval * heartbeatAllowedMisses < currentTime) {
                    LOGGER.atInfo().log("Connection timed out, reconnecting");
                    resetConnection();
                }
                // check timeouts of connection/subscription requests
                for (Subscription sub : subscriptions.values()) {
                    switch (sub.subscriptionState) {
                    case SUBSCRIBING:
                        // check timeout
                        if (currentTime > sub.timeoutValue) {
                            sub.subscriptionState = SubscriptionState.UNSUBSCRIBED;
                            sub.timeoutValue = currentTime + backOff;
                            backOff = backOff * 2; // exponential back of
                            LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("subscription timed out, retrying: {}/{}");
                        }
                        break;
                    case UNSUBSCRIBED:
                        if (currentTime > sub.timeoutValue) {
                            LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("subscribing {}/{}");
                            sendSubscribe(sub);
                        }
                        break;
                    case SUBSCRIBED:
                        // do nothing
                        break;
                    default:
                        throw new IllegalStateException("unexpected subscription state: " + sub.subscriptionState);
                    }
                }
            }
            return lastHbSent + heartbeatInterval;
        default:
            throw new IllegalStateException("unexpected connection state: " + connectionState.get());
        }
    }

    public void subscribe(final String device, final String property, final String selector) {
        subscribe(device, property, selector, null);
    }

    public void subscribe(final String device, final String property, final String selector, final Map<String, Object> filters) {
        subscribe(new Subscription(device, property, selector, filters));
    }

    public <T> void subscribe(final String device, final String property, final String selector, final Map<String, Object> filters, final Class<T> domainClass) {
        subscribe(new Subscription(device, property, selector, filters, domainClass));
    }

    public void subscribe(final Subscription sub) {
        subscriptions.put(sub.id, sub);
    }

    public void sendHeartBeat() {
        try {
            CmwLightProtocol.sendMsg(controlChannel, CmwLightMessage.CLIENT_HB);
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("Error sending heartbeat");
        }
    }

    private void sendSubscribe(final Subscription sub){
        if (!sub.subscriptionState.equals(SubscriptionState.UNSUBSCRIBED)) {
            return; // already subscribed/subscription in progress
        }
        try {
            CmwLightProtocol.sendMsg(controlChannel, CmwLightMessage.subscribeRequest(
                    sessionId, sub.id, sub.device, sub.property,
                    Map.of(CmwLightProtocol.FieldName.SESSION_BODY_TAG.value(), Collections.<String, Object>emptyMap()),
                    new CmwLightMessage.RequestContext(sub.selector, sub.filters, null),
                    CmwLightProtocol.UpdateType.IMMEDIATE_UPDATE));
            sub.subscriptionState = SubscriptionState.SUBSCRIBING;
            sub.timeoutValue = System.currentTimeMillis() + subscriptionTimeout;
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("Error subscribing to property:");
            sub.timeoutValue = System.currentTimeMillis() + sub.backOff;
            sub.backOff *= 2;
        }
    }

    private void sendUnsubscribe(final Subscription sub) throws CmwLightProtocol.RdaLightException {
        if (sub.subscriptionState.equals(SubscriptionState.UNSUBSCRIBED)) {
            return; // not currently subscribed to this property
        }
        CmwLightProtocol.sendMsg(controlChannel, CmwLightMessage.unsubscribeRequest(
                                                         sessionId, sub.updateId, sub.device, sub.property,
                                                         Map.of(CmwLightProtocol.FieldName.SESSION_BODY_TAG.value(), Collections.<String, Object>emptyMap()),
                                                         CmwLightProtocol.UpdateType.IMMEDIATE_UPDATE));
    }

    public static class Subscription {
        public final String property;
        public final String device;
        public final String selector;
        public final Map<String, Object> filters;
        private SubscriptionState subscriptionState = SubscriptionState.UNSUBSCRIBED;
        private int backOff = 20;
        private final long id = requestIdGenerator.incrementAndGet();
        private long updateId = -1;
        private long timeoutValue = -1;
        private Class<?> domainClass;

        public Subscription(final String device, final String property, final String selector, final Map<String, Object> filters, final Class<?> domainClass) {
            this.property = property;
            this.device = device;
            this.selector = selector;
            this.filters = filters;
            this.domainClass = domainClass;
        }

        public Subscription(final String device, final String property, final String selector, final Map<String, Object> filters) {
            this.property = property;
            this.device = device;
            this.selector = selector;
            this.filters = filters;
        }

        @Override
        public String toString() {
            return "Subscription{"
                    + "property='" + property + '\'' + ", device='" + device + '\'' + ", selector='" + selector + '\'' + ", filters=" + filters + ", subscriptionState=" + subscriptionState + ", backOff=" + backOff + ", id=" + id + ", updateId=" + updateId + ", timeoutValue=" + timeoutValue + '}';
        }
    }

    public static class DataSourceFilter implements Filter {
        public Class<?> requestedDomainObjType;
        public EventType eventType = EventType.UNKNOWN;
        public Class<? extends IoSerialiser> protocolType;


        @Override
        public void clear() {
            requestedDomainObjType = null;
            eventType = EventType.UNKNOWN;
        }

        @Override
        public void copyTo(final Filter other) {
            if (other instanceof DataSourceFilter) {
                final DataSourceFilter otherDSF = (DataSourceFilter) other;
                otherDSF.eventType = eventType;
                otherDSF.requestedDomainObjType = requestedDomainObjType;
            }
        }
    }

    public enum EventType {
        NOTIFICATION_UPDATE, GET_REPLY, SET_REPLY, STATE_CHANGE, UNKNOWN
    }

    public enum SubscriptionState {
        UNSUBSCRIBED,
        SUBSCRIBING,
        SUBSCRIBED
    }
}
