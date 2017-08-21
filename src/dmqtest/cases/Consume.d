/******************************************************************************

    Collections of test cases for DMQ Consume behavior

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dmqtest.cases.Consume;

/******************************************************************************

    Imports

******************************************************************************/

import dmqtest.DmqTestCase;
import ocean.transition;

/******************************************************************************

    Adding value to the queue must affect the consumer

******************************************************************************/

class PushTriggersConsumer : DmqTestCase
{
    DmqClient.Consumer consumer;

    override public Description description ( )
    {
        Description desc;
        desc.name = "Consume must received pushed value";
        return desc;
    }

    override public void run ( )
    {
        this.consumer = this.dmq.startConsume(this.channels[0]);

        this.dmq.push(this.channels[0], "something"[]);
        consumer.waitNextEvent();
        test(!consumer.finished);
        test!("==")(consumer.data.length, 1);
        test!("==")(consumer.data[0], "something");
    }

    override public void cleanup ( )
    {
        // issue removeChannel request
        super.cleanup();
        // wait for stream finished signal from node
        this.consumer.waitNextEvent();
    }
}

/******************************************************************************

    Ensures that consuming will stop on removing the channel

******************************************************************************/

class ConsumeRemoveChannel : DmqTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.name = "Consume stops after removing the channel";
        return desc;
    }

    override public void run ( )
    {
        auto consumer = this.dmq.startConsume(this.channels[0]);

        this.dmq.push(this.channels[0], "something"[]);
        consumer.waitNextEvent();
        consumer.data.length = 0;
        enableStomping(consumer.data);
        test(!consumer.finished);

        this.dmq.removeChannel(this.channels[0]);
        consumer.waitNextEvent();
        test(consumer.finished);
    }
}
