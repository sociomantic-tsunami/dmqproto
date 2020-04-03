/*******************************************************************************

    Ensures that it is possible to push more values into fakedmq than internal
    storage limit allows as long as there is consumer working in parallel.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.dmqfull.main;

import ocean.core.Enforce;
import ocean.task.Task;
import ocean.task.Scheduler;

import dmqproto.client.DmqClient;

import turtle.env.Dmq;
import fakedmq.Storage;

import ocean.io.Stdout;

static immutable RECORD_COUNT = 10000;

version ( unittest ) {} else
void main ()
{
    global_storage.channel_size_limit = RECORD_COUNT / 10;

    initScheduler(SchedulerConfiguration.init);
    auto task = new TestTask;
    theScheduler.schedule(task);
    theScheduler.eventLoop();
    enforce!("==")(task.records, RECORD_COUNT);
}

class TestTask : Task
{
    long records = 0;

    void notifier (DmqClient.RequestNotification info)
    {
        enforce(info.type != info.type.Finished || info.succeeded);
    }

    void consumer (DmqClient.RequestContext, in char[] data)
    {
        ++records;
        if (records == RECORD_COUNT)
            theScheduler.shutdown();
    }

    override void run ( )
    {
        theScheduler.processEvents();

        Dmq.initialize();
        dmq.start("127.0.0.1");

        auto reader = new DmqClient(theScheduler.epoll());
        reader.addNode("127.0.0.1".dup, dmq.node_addrport.port);
        reader.assign(reader.consume("test", &consumer, &notifier));

        for (int i; i < RECORD_COUNT; ++i)
            dmq.push("test", [ 42 ]);
    }
}
