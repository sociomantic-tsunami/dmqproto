/*******************************************************************************

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.mixins.RequestConstruction;

public template RequestConstructor()
{
    import dmqproto.node.request.model.DmqCommand;
    import ocean.io.select.client.FiberSelectEvent;

    /***************************************************************************

        Event required by some requests in order to suspend the fiber until
        the event is triggered.

    ***************************************************************************/

    FiberSelectEvent event;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        FiberSelectEvent event, DmqCommand.Resources resources )
    {
        super(reader, writer, resources);
        this.event = event;
    }
}
