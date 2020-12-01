package de.gsi.microservice.datasource;

import com.lmax.disruptor.EventHandler;
import de.gsi.microservice.EventStore;
import de.gsi.microservice.RingBufferEvent;
import de.gsi.microservice.datasource.cmwlight.CmwLightClient;
import de.gsi.microservice.datasource.cmwlight.CmwLightMessage;
import de.gsi.microservice.filter.EvtTypeFilter;
import de.gsi.microservice.filter.TimingCtx;
import de.gsi.microservice.utils.SharedPointer;
import de.gsi.serializer.IoClassSerialiser;
import de.gsi.serializer.spi.FastByteBuffer;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sample implementation for subscription to multiple cmw, rest, mpd servers and properties or replay data saved to files
 * and publishing the updates into a high performance ring buffer for further processing.
 */
public class DataSourcePublisher implements Runnable {
    private final EventStore eventStore;
    private final Map<Integer, DataSource> clients = new HashMap<>(); // poller index -> client
    private final ZMQ.Poller poller;
    private final ZMQ.Context context;
    private final IoClassSerialiser ioClassSerialiser = new IoClassSerialiser(new FastByteBuffer(0));
    private final AtomicBoolean running = new AtomicBoolean(false);

    // TODO: add control channel? or add to individual clients? get, subscribe, set, (re)connect


    public DataSourcePublisher(final EventStore publicationTarget) {
        this();
        eventStore.register((event, sequence, endOfBatch) -> {
            final CmwLightClient.DataSourceFilter dataSourceFilter = event.getFilter(CmwLightClient.DataSourceFilter.class);
            if (dataSourceFilter.eventType.equals(CmwLightClient.EventType.NOTIFICATION_UPDATE)){
                final Class<?> domainClass = dataSourceFilter.requestedDomainObjType;
                final CmwLightMessage cmwMsg = event.payload.get(CmwLightMessage.class);
                ioClassSerialiser.setDataBuffer(FastByteBuffer.wrap(cmwMsg.bodyData.getData()));
                final Object domainObj = ioClassSerialiser.deserialiseObject(domainClass);
                publicationTarget.getRingBuffer().publishEvent((publishEvent, seq, obj, msg) -> {
                    final TimingCtx contextFilter = publishEvent.getFilter(TimingCtx.class);
                    final EvtTypeFilter evtTypeFilter = publishEvent.getFilter(EvtTypeFilter.class);
                    publishEvent.arrivalTimeStamp = event.arrivalTimeStamp;
                    publishEvent.payload = new SharedPointer<>();
                    publishEvent.payload.set(obj);
                    contextFilter.selector = msg.dataContext.cycleName;
                    contextFilter.bpcts = msg.dataContext.cycleStamp;
                    // contextFilter.acqts = msg.dataContext.acqStamp; // needs to be added?
                    // contextFilter.cid =  // get cid from context or from Acq property if instanceof Acquisition?
                    // contextFilter.gid =
                    // contextFilter.sid =
                    // contextFilter.pid =
                    // contextFilter.ctxName = // what should go here?
                    evtTypeFilter.evtType = EvtTypeFilter.DataType.DEVICE_DATA;
                    // evtTypeFilter.typeName = ""; // todo
                    evtTypeFilter.updateType = EvtTypeFilter.UpdateType.COMPLETE;
                }, domainObj, cmwMsg);
            } else {
                // todo: publish statistics, connection state and getRequests
                System.out.println(event.payload.get());
            }
        });
    }

    public DataSourcePublisher() {
        context = ZMQ.context(1);
        poller = context.poller();
        eventStore = EventStore.getFactory().setSingleProducer(true).setFilterConfig(CmwLightClient.DataSourceFilter.class).build();
    }

    public DataSourcePublisher(final EventHandler<RingBufferEvent> eventHandler) {
        context = ZMQ.context(1);
        poller = context.poller();
        eventStore = EventStore.getFactory().setSingleProducer(true).setFilterConfig(CmwLightClient.DataSourceFilter.class).build();
        eventStore.register(eventHandler);
    }

    public EventStore getEventStore() {
        return eventStore;
    }

    public int add(DataSource client) {
        final int index = poller.register(client.getSocket(), ZMQ.Poller.POLLIN);
        clients.put(index, client);
        return index;
    }

    @Override
    public void run() {
        // start the ring buffer and its processors
        eventStore.start();
        // event loop polling all data sources and performing regular housekeeping jobs
        running.set(true);
        long nextHousekeeping = System.currentTimeMillis(); // immediately perform first housekeeping
        while (!Thread.interrupted() && running.get()) { // todo add run condition
            if (poller.poll(nextHousekeeping - System.currentTimeMillis()) > 0) {
                // get data from clients
                boolean dataAvailable = true;
                while (dataAvailable && System.currentTimeMillis() < nextHousekeeping && running.get()) {
                    dataAvailable = false;
                    for (Map.Entry<Integer, DataSource> entry : clients.entrySet()) {
                        if (poller.pollin(entry.getKey())) {
                            dataAvailable = dataAvailable || entry.getValue().poll(eventStore);
                        }
                    }
                }
            }
            nextHousekeeping = System.currentTimeMillis() + 1000;
            for (DataSource client : clients.values()) {
                nextHousekeeping = Math.min(nextHousekeeping, client.doHousekeeping(System.currentTimeMillis()));
            }
        }
    }

    public ZMQ.Context getContext() {
        return context;
    }

    /**
     * Perform an asynchronous get request on the given device/property.
     * Checks if a client for this service already exists and if it does performs the asynchronous get on it, otherwise
     * it starts a new client and performs it there.
     *
     * @param address Address for the property e.g. rda3://hostname:port, file:///path/to/directory, mdp://host:port
     * @param device Device name
     * @param property property name
     * @param selector selector
     * @param filters map of optional filters e.g. Map.of("channelName", "VoltageChannel")
     * @param domainClass the class the result should be deserialised into
     * @param <T> The type of the deserialised class
     * @return A future which will be able to retrieve the deserialised result
     */
    public <T> Future<T> get(final String address, final String device, final String property, final String selector, final Map<String, Object> filters, final Class<T> domainClass) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Subscribe to a specific service and device/property.
     * TODO: how to call this from different thread
     *
     * @param address
     * @param device
     * @param property
     * @param selector
     * @param filters
     * @param domainClass
     * @param <T>
     */
    public <T> void subscribe(final String address, final String device, final String property, final String selector, final Map<String, Object> filters, final Class<T> domainClass) {
        final DataSource dataSource;
        if (CmwLightClient.matches(address)) { // todo: use automatic service discovery?
            dataSource = new CmwLightClient(address, this.getContext());
            ((CmwLightClient) dataSource).subscribe(device, property, selector, filters, domainClass);
        } else {
            throw new UnsupportedOperationException("unsupported address type: " + address);
        }
        add(dataSource);
    }
}
