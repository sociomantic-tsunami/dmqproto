/*******************************************************************************

    Neo DHT client shared resources, available to all request handlers.

    Copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.internal.SharedResources;

import ocean.transition;

/// ditto
public final class SharedResources
{
    import swarm.neo.util.AcquiredResources;
    import swarm.neo.request.RequestEventDispatcher;
    import swarm.neo.util.MessageFiber;

    import ocean.util.container.pool.FreeList;
    import ocean.core.TypeConvert : downcast;
    import ocean.io.compress.Lzo;

    /// Free list of recycled buffers
    private FreeList!(ubyte[]) buffers;

    /// Pool of RequestEventDispatcher instances.
    private FreeList!(RequestEventDispatcher) request_event_dispatchers;

    /// Pool of MessageFiber instances.
    private FreeList!(MessageFiber) fibers;

    /// Lzo instance shared by all record batches (newed on demand)
    private Lzo lzo_;

    /***************************************************************************

        A SharedResources instance is stored in the ConnectionSet as an Object.
        This helper function safely casts from this Object to a correctly-typed
        instance.

        Params:
            obj = object to cast from

        Returns:
            obj cast to SharedResources

    ***************************************************************************/

    public static typeof(this) fromObject ( Object obj )
    {
        auto shared_resources = downcast!(typeof(this))(obj);
        assert(shared_resources !is null);
        return shared_resources;
    }

    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        this.buffers = new FreeList!(ubyte[]);
        this.request_event_dispatchers = new FreeList!(RequestEventDispatcher);
        this.fibers = new FreeList!(MessageFiber);
    }

    /***************************************************************************

        Class to track the resources acquired by a request and relinquish them
        (recylcing them into the shared resources pool) when the request
        finishes. An instance should be newed as a request is started and
        destroyed as it finishes. Newing an instance as `scope` is the most
        convenient way.

    ***************************************************************************/

    public class RequestResources
    {
        /// Set of acquired buffers
        private AcquiredArraysOf!(void) acquired_buffers;

        /// Singleton RequestEventDispatcher used by this request.
        private AcquiredSingleton!(RequestEventDispatcher)
            acquired_request_event_dispatcher;

        /// Set of acquired fibers.
        private Acquired!(MessageFiber) acquired_fibers;

        /***********************************************************************

            Constructor.

        ***********************************************************************/

        this ( )
        {
            this.acquired_buffers.initialise(this.outer.buffers);
            this.acquired_fibers.initialise(this.outer.buffers,
                this.outer.fibers);
            this.acquired_request_event_dispatcher.initialise(
                this.outer.request_event_dispatchers);
        }

        /***********************************************************************

            Destructor. Relinquishes any acquired resources.

        ***********************************************************************/

        ~this ( )
        {
            this.acquired_buffers.relinquishAll();
            this.acquired_fibers.relinquishAll();
            this.acquired_request_event_dispatcher.relinquish();
        }

        /***********************************************************************

            Returns:
                a new buffer acquired from the shared resources pools

        ***********************************************************************/

        public void[]* getBuffer ( )
        {
            return this.acquired_buffers.acquire();
        }

        /***********************************************************************

            Returns:
                pointer to singleton (one per request) RequestEventDispatcher
                instance

        ***********************************************************************/

        public RequestEventDispatcher* request_event_dispatcher ( )
        {
            return this.acquired_request_event_dispatcher.acquire(
                new RequestEventDispatcher,
                ( RequestEventDispatcher* dispatcher )
                {
                    dispatcher.reset();
                }
            );
        }

        /***********************************************************************

            Gets a fiber to use during the request's lifetime and assigns the
            provided delegate as its entry point.

            Params:
                fiber_method = entry point to assign to acquired fiber

            Returns:
                a new MessageFiber acquired to use during the request's lifetime

        ***********************************************************************/

        public MessageFiber getFiber ( void delegate ( ) fiber_method )
        {
            bool new_fiber = false;

            MessageFiber newFiber ( )
            {
                new_fiber = true;
                return new MessageFiber(fiber_method, 64 * 1024);
            }

            auto fiber = this.acquired_fibers.acquire(newFiber());
            if ( !new_fiber )
                fiber.reset(fiber_method);

            return fiber;
        }

        /***********************************************************************

            Returns:
                shared Lzo instance

        ***********************************************************************/

        public Lzo getLzo ( )
        {
            if ( this.outer.lzo_ is null )
                this.outer.lzo_ = new Lzo;

            return this.outer.lzo_;
        }
    }
}
