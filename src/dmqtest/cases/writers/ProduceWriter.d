/*******************************************************************************

    Writer which uses the Produce request.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.cases.writers.ProduceWriter;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqtest.cases.writers.model.IWriter;

abstract class ProduceWriterBase : IWriter
{
    /***************************************************************************

        The producer that sends records to the DMQ.

    ***************************************************************************/

    protected DmqClient.Producer producer;

    /***************************************************************************

        Constructor, sets the channels.

        Params:
            channels = list of channel names to write to

    ***************************************************************************/

    this ( Const!(char[])[] channels ... ) {super(channels);}

    /***************************************************************************

        Sends records to the DMQ using Produce and updates the local store
        in the same way.
        Should be called after `this.prepare()` has returned.

        Params:
            local = local store to contain the expected state of the DMQ

    ***************************************************************************/

    override public void run ( Const!(char[])[] records )
    {
        assert(this.producer is null);
        this.producer = this.startProduce();
        scope (exit) this.producer = null;
        super.run(records);
        this.producer.finish();
    }

    /***************************************************************************

        Starts a producer that will send records to all channels in
        `this.channel`. It is safe to assume `this.dmq` is ready to use.

        Returns:
            the producer.

    ***************************************************************************/

    abstract protected DmqClient.Producer startProduce ( );

    /***************************************************************************

        Sends `record` to the DMQ.
        It is safe to assume `this.producer` is the object returned by
        `this.startProduce()`. In `startProduce()` the subclass needs to make
        sure records will go to all channels in `this.channels`.

        Params:
            record = the record to send

    ***************************************************************************/

    override protected void dmqSend ( cstring record )
    {
        assert(this.producer);
        this.producer.write(record);
    }
}

class ProduceWriter : ProduceWriterBase
{
    /***************************************************************************

        Constructor.

    ***************************************************************************/

    this ( )
    {
        super("test_channel");
    }

    /***************************************************************************

        Starts a producer that will send records to all channels in
        `this.channel`.

        Returns:
            the producer.

    ***************************************************************************/

    override protected DmqClient.Producer startProduce ( )
    {
        assert(this.test_channels.length == 1); // as specified in the constructor
        return this.dmq.startProduce(this.test_channels[0]);
    }
}
