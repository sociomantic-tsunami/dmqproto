/*******************************************************************************

    Helper class, used by produce requests, which provides the functionality of
    sending an IProducer interface to a user-provided delegate, then waiting
    until the interface's produce() method is called, providing a value.

    Copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.helper.ValueProducer;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.client.legacy.internal.request.model.IProducer;

import swarm.client.request.model.IFlushable;

import swarm.protocol.FiberSelectWriter;

import dmqproto.client.legacy.internal.request.params.RequestParams;

import swarm.client.request.context.RequestContext;

import swarm.Const : NodeItem;

import ocean.core.Array : copy;

import ocean.io.select.client.FiberSelectEvent;

debug import ocean.io.Stdout;



public class ValueProducer : IProducer, IFlushable
{
    /***************************************************************************

        Fiber select event used to wait until a value is ready to be sent.

        Note that the event instance is not const, as it is occasionally useful
        to be able to change the event after construction. An example of this
        use case would be when a value producer instance is created for use with
    	a request, but then, some time later, needs to be re-used for a
    	different request - necessitating an event switch.

    ***************************************************************************/

    public FiberSelectEvent event;


    /***************************************************************************

        Fiber select writer to flush when requested.

        Note that the writer instance is not const, as it is occasionally useful
        to be able to change the writer after construction. An example of this
        use case would be when a value producer instance is created for use with
    	a request, but then, some time later, needs to be re-used for a
    	different request - necessitating a writer switch.

    ***************************************************************************/

    public FiberSelectWriter writer;


    /***************************************************************************

        The event and writer must always be non-null.

    ***************************************************************************/

    invariant ()
    {
        assert(this.event !is null, typeof(this).stringof ~ " event is null");
        assert(this.writer !is null, typeof(this).stringof ~ " writer is null");
    }


    /***************************************************************************

        Set to true when the produce() method has been called, denoting that a
        value is ready to be sent.

    ***************************************************************************/

    private bool value_ready;


    /***************************************************************************

        Value to send to node.

    ***************************************************************************/

    private mstring value;


    /***************************************************************************

        Set to true when the flush() method has been called, denoting that a
        flush should be performed when possible. Note that this flag will be set
        to true even if the writer is in the middle of sending a value (i.e. if
        busy and value_ready are true). In this case a flush will be performed
        the next time the next() method begins.

    ***************************************************************************/

    private bool flush_requested;


    /***************************************************************************

        Flag set to true when the fiber is busy doing something (either a value
        is being sent or the writer is being flushed). Used to prevent a new
        value or buffer flush request from interrupting an on-going value or
        flush.

    ***************************************************************************/

    private bool busy;


    /***************************************************************************

        The node item this producer is associated with

    ***************************************************************************/

    public NodeItem nodeitem_;


    /***************************************************************************

        The RequestContext associated with this instance

    ***************************************************************************/

    public RequestContext context_;


    /***************************************************************************

        Constructor

        Params:
            writer = FiberSelectWriter instance to flush when requested (note
                that this class never writes anything to this writer instance,
                it is solely used for flushing)
            event = FiberSelectEvent instance used to wait for data / flush
                triggers
            ni = nodeitem this producer is associated with
            c  = context this producer is asosciated with

    ***************************************************************************/

    public this ( FiberSelectWriter writer, FiberSelectEvent event, NodeItem ni,
                  RequestContext c)
    {
        this.writer = writer;
        this.event = event;
        this.nodeitem_ = ni;
        this.context_ = c;
    }


    /***************************************************************************

        IProducer interface method. Provides a value to be sent.

        Note that this method can be called out of sequence with the fiber
        method (next(), below), thus we need to be careful to not touch the
        fiber from here.

        Params:
            value = value to send to node

        Returns:
            true if the value is accepted to be sent, false if the request is
            already sending a previous value

    ***************************************************************************/

    public bool produce ( cstring value )
    {
        if ( !this.busy )
        {
            this.busy = true;
            this.value_ready = true;
            this.value.copy(value);
            this.event.trigger();
            return true;
        }

        return false;
    }


    /***************************************************************************

        IFlushable interface method. Requests that the write buffer be flushed.

        Note that this method can be called out of sequence with the fiber
        method (next(), below), thus we need to be careful to not touch the
        fiber from here.

    ***************************************************************************/

    public void flush ( )
    {
        this.flush_requested = true;

        if ( !this.busy )
        {
            this.busy = true;
            this.event.trigger();
        }
    }


    /***************************************************************************

        Passes an IProducer interface to this instance to the provided delegate.
        Suspends the fiber until the produce() method of the interface is
        called, providing a value to send, or the flush() method is called,
        requesting the write buffer to be flushed. When a value is passed to
        produce(), it is subsequently returned.

        This method is aliased as opCall.

        Params:
            ready_for_data = delegate to pass IProducer interface to
            context = context to pass to delegate

        Returns:
            value passed to produce(), above

    ***************************************************************************/

    public cstring next ( scope RequestParams.ProducerDg producer_dg,
        RequestContext context )
    {
        this.value_ready = false;
        this.busy = false;

        producer_dg(context, this);

        do
        {
            if ( this.flush_requested )
            {
                this.writer.flush();
                this.flush_requested = false;
                this.busy = this.value_ready;
            }

            this.event.wait; // wait for something to happen
        }
        while ( !this.value_ready );

        return this.value;
    }

    public alias next opCall;


    /***************************************************************************

        sets the context for this producer

    ***************************************************************************/

    public void context ( RequestContext c )
    {
        this.context_ = c;
    }


    /***************************************************************************

        Returns the context set for this producer

    ***************************************************************************/

    public RequestContext context ( )
    {
        return this.context_;
    }


    /***************************************************************************

        Returns the nodeitem this producer is associated with

    ***************************************************************************/

    public NodeItem nodeitem ( )
    {
        return this.nodeitem_;
    }
}
