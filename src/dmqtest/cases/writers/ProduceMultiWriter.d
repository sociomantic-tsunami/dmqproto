/*******************************************************************************

    Writer which uses the ProduceMulti request.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.cases.writers.ProduceMultiWriter;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqtest.cases.writers.ProduceWriter;

class ProduceMultiWriter : ProduceWriterBase
{
    /***************************************************************************

        Constructor.

    ***************************************************************************/

    this ( )
    {
        super("test_channel1", "test_channel2");
    }

    /***************************************************************************

        Starts a producer that will send records to all channels in
        `this.channel`.

        Returns:
            the producer.

    ***************************************************************************/

    override protected DmqClient.Producer startProduce ( )
    {
        return this.dmq.startProduceMulti(this.test_channels);
    }
}
