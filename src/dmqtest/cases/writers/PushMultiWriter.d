/*******************************************************************************

    Writer which uses the PushMulti request.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.cases.writers.PushMultiWriter;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqtest.cases.writers.model.IWriter;

import ocean.transition;


class PushMultiWriter : IWriter
{
    /***************************************************************************

        Constructor.

    ***************************************************************************/

    this ( )
    {
        super("test_channel1", "test_channel2");
    }

    /***************************************************************************

        Sends `record` to the DMQ using PushMulti.

        Params:
            record = the record to send

    ***************************************************************************/

    override protected void dmqSend ( cstring record )
    {
        this.dmq.pushMulti(this.test_channels, record);
    }
}

