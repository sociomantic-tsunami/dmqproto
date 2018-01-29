/*******************************************************************************

    Checker which uses the Pop request.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.cases.checkers.PopChecker;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqtest.cases.checkers.model.IChecker;

private abstract class PopCheckerBase : IChecker
{
    /***************************************************************************

        List of channels to be popped from. Set in the constructor.

    ***************************************************************************/

    public Const!(char[][]) channels;

    /***************************************************************************

        DMQ client to use. Set in `prepare()`.

    ***************************************************************************/

    protected DmqClient dmq;

    /***************************************************************************

        Constructor, sets the channels.

        Params:
            test_channels = channels to pop from

    ***************************************************************************/

    public this ( Const!(char[])[] test_channels )
    {
        this.channels = test_channels;
    }

    /***************************************************************************

        Sets the DMQ client to use.
        Should be called before calling `this.check()`.

        Params:
            DMQ = DMQ client

    ***************************************************************************/

    override public void prepare ( DmqClient dmq )
    {
        this.dmq = dmq;
    }

    /***************************************************************************

        Checks the data stored in the node matches what's in the local store.
        Should be called after `this.prepare()` has returned.

        Params:
            local = local store to contain the expected state of the DMQ

        Throws:
            TestException if the check fails

    ***************************************************************************/

    override public void check ( Const!(char[])[] records )
    {
        foreach ( channel; this.channels )
        {
            foreach ( expected; records )
            {
                test!("==")(this.dmqPop(channel), expected);
            }

            test!("==")(this.dmqPop(channel).length, 0);
        }
    }

    /***************************************************************************

        Pops a record from `channel`.
        It is safe to assume `this.dmq` is ready to use.

        Params:
            channel = the channel to pop from

    ***************************************************************************/

    abstract protected void[] dmqPop ( cstring channel );
}

class PopChecker : PopCheckerBase
{
    /***************************************************************************

        Constructor. This class doesn't need to perform any logic at startup.

        Params:
            test_channels = channels to consume from

    ***************************************************************************/

    public this ( Const!(char[])[] test_channels )
    {
        super(test_channels);
    }

    /***************************************************************************

        Pops a record from `channel`.
        It is safe to assume `this.dmq` is ready to use.

        Params:
            channel = the channel to pop from

    ***************************************************************************/

    override protected void[] dmqPop ( cstring channel )
    {
        return this.dmq.pop(channel);
    }
}
