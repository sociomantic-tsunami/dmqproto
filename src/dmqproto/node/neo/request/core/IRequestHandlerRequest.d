/*******************************************************************************

    Base class for requests. Implements IRequestHandler and leaves one method,
    `handle`, to be implemented by the subclass.

    Copyright:
        Copyright (c) 2016-2018 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.neo.request.core.IRequestHandlerRequest;

import swarm.neo.node.IRequestHandler;

/// ditto
abstract class IRequestHandlerRequest: IRequestHandler
{
    import swarm.neo.node.RequestOnConn;
    import dmqproto.node.neo.request.core.IRequestResources;
    import ocean.core.Verify;
    import ocean.transition;

    /// Request-on-conn of this request handler.
    private RequestOnConn connection;

    /// Acquired resources of this request.
    private IRequestResources resources;

    /***************************************************************************

        The payload of the initial message.
        This array references an acquired buffer so do not change the length.

    ***************************************************************************/

    private Const!(void)[] msg_payload;

    /***************************************************************************

        Stores the request-on-conn and request resource acquirer, to be passed
        to `handle`.

        Params:
            connection = request-on-conn in which the request handler is called
            resources = request resources acquirer

    ***************************************************************************/

    public void initialise ( RequestOnConn connection, Object resources_object )
    {
        this.connection = connection;
        this.resources = cast(IRequestResources)resources_object;
        verify(this.resources !is null);
    }

    /***************************************************************************

        Copies `init_payload` into an acquired buffer, to be passed to `handle`.

        Params:
            init_payload = initial message payload read from the connection

    ***************************************************************************/

    public void preSupportedCodeSent ( Const!(void)[] init_payload )
    {
        void[]* buf = this.resources.getVoidBuffer();
        (*buf).length = init_payload.length;
        (*buf)[] = init_payload;
        this.msg_payload = *buf;
    }

    /***************************************************************************

        Calls `handle` with the objects passed to `initialise` and
        `preSupportedCodeSent`, then relinquishes the objects.

    ***************************************************************************/

    public void postSupportedCodeSent ( )
    {
        try
            this.handle(this.connection, this.resources, this.msg_payload);
        finally
        {
            this.connection = null;
            this.resources = null;
            // this.msg_payload is relinquished automatically after this
            // method has returned.
        }
    }

    /***************************************************************************

        Request handler.

        Params:
            connection = connection to client
            resources = request resources acquirer
            msg_payload = initial message read from client to begin the request
                (the request code and version are assumed to be extracted)

    ***************************************************************************/

    abstract protected void handle ( RequestOnConn connection,
        IRequestResources resources, Const!(void)[] msg_payload );
}
