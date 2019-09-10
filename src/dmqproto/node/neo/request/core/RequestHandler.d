/*******************************************************************************

    Base class for requests. Implements IRequestHandler and leaves one method,
    `handle`, to be implemented by the subclass.

    Copyright:
        Copyright (c) 2016-2018 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.neo.request.core.RequestHandler;

import swarm.neo.node.IRequest;

/// ditto
abstract class RequestHandler: IRequest
{
    import swarm.neo.node.RequestOnConn;
    import dmqproto.node.neo.request.core.IRequestResources;
    import ocean.core.Verify;
    import ocean.transition;

    /// Request-on-conn of this request handler.
    protected RequestOnConn connection;

    /// Acquired resources of this request.
    private IRequestResources resources;

    /***************************************************************************

        The payload of the initial message.
        This array references an acquired buffer so do not change the length.

    ***************************************************************************/

    private Const!(void)[] msg_payload;

    /***************************************************************************

        Request-on-conn event dispatcher, to send and receive messages.

    ***************************************************************************/

    protected RequestOnConn.EventDispatcher ed;

    /***************************************************************************

        Called by the connection handler after the request code and version have
        been parsed from a message received over the connection, and the
        request-supported code sent in response.

        Stores the request-on-conn and request resource acquirer in the current
        member.

        Note: the initial payload passed to this method is a slice of a buffer
        owned by the RequestOnConn. It is thus safe to assume that the contents
        of the buffer will not change over the lifetime of the request.

        Params:
            connection = request-on-conn in which the request handler is called
            resources = request resources acquirer
            init_payload = initial message payload read from the connection

    ***************************************************************************/

    public void handle ( RequestOnConn connection, Object resources,
        Const!(void)[] init_payload )
    {
        this.connection = connection;
        this.resources = cast(IRequestResources)resources;
        verify(this.resources !is null);

        this.ed = connection.event_dispatcher;

        void[]* buf = this.resources.getVoidBuffer();
        (*buf).length = init_payload.length;
        (*buf)[] = init_payload;
        this.msg_payload = *buf;

        this.handle(this.connection, this.resources, this.msg_payload);
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
