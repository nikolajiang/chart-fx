package de.gsi.microservice.datasource;

import de.gsi.microservice.EventStore;
import org.zeromq.ZMQ;

import java.nio.channels.SelectableChannel;

/**
 * Interface for DataSources to be added to an EventStore by a single event loop.
 * Should provide a static boolean matches(String address) function to determine whether
 * it is eligible for a given address.
 */
public interface DataSource {
    /**
     * Get Socket to wait for in the event loop
     * @return a Socket for the event loop to wait upon
     */
    ZMQ.Socket getSocket();

    /**
     * Poll the connection's socket for new data and publish to event queue
     * @param eventStore eventStore to publish new values into
     * @return true if there was data read
     */
    boolean poll(final EventStore eventStore);

    /**
     * Do housekeeping like connection management, heartbeats, subscriptions, etc
     * @param currentTime the current time
     * @return next time housekeeping duties should be performed
     */
    long doHousekeeping(final long currentTime);

    /**
     * @return the address this DataSource is connected to/reading from.
     */
    String getAddress();
}