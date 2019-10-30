/*******************************************************************************

    Base class for writers, to send some data to the DMQ for a test.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.cases.writers.model.IWriter;

abstract class IWriter
{
    import dmqtest.DmqClient;
    import ocean.transition;

    /***************************************************************************

        List of channels to be written to. Set in the constructor.

    ***************************************************************************/

    public const(char[])[] test_channels;

    /***************************************************************************

        DMQ client to use. Set in `prepare()`.

    ***************************************************************************/

    protected DmqClient dmq;

    /***************************************************************************

        Constructor.

        Params:
            test_channels = list of channel names to write to

    ***************************************************************************/

    public this ( const(char[])[] test_channels ... )
    {
        this.test_channels = test_channels.dup;
    }

    /***************************************************************************

        Sets the DMQ client to use.
        Should be called before calling `this.run()`.

        Params:
            DMQ = DMQ client

    ***************************************************************************/

    public void prepare ( DmqClient dmq )
    {
        this.dmq = dmq;
    }

    /***************************************************************************

        Sends records to the and updates the local store in the same way.
        Should be called after `this.prepare()` has returned.

        Params:
            local = local store to contain the expected state of the DMQ

    ***************************************************************************/

    public void run ( const(char[])[] records )
    in
    {
        assert(this.dmq, "Call prepare first");
    }
    body
    {
        foreach (record; records)
            this.dmqSend(record);
    }

    /***************************************************************************

        Sends `record` to all DMQ channels in `this.channel`.
        It is safe to assume `this.dmq` is ready to use.

        Params:
            record = the record to send

    ***************************************************************************/

    abstract protected void dmqSend ( cstring record );
}
