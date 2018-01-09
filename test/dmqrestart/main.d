/*******************************************************************************

    Tests the behaviour of the fake DMQ upon shutting down and restarting.

    This test does the following:
        * Starts a fake DMQ node.
        * Pushes records to it with a DMQ client.
        * Every 10 Push requests, shuts down and restarts the node.
        * Checks that the expected number of records reach the node and that the
          expected number of errors occur in the DMQ client.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

import ocean.core.Enforce;
import ocean.task.Scheduler;
import ocean.task.Task;

import dmqproto.client.DmqClient;

import turtle.env.Dmq;

void main ( )
{
    class TestTask : Task
    {
        /** Maximum number of connections per node in the DMQ client.

            FIXME: The test should work with only a single connection. However,
            a bug in the legacy DMQ client causes the following behaviour after
            restarting the node:
              1. A request is assigned to a now-dead connection.
              2. The request fails (as expected).
              3. Faulty round-robin logic in the DMQ client reassigns the
                 request to the same node, pushing it to the request queue.
              4. This continues through the whole test until the last request is
                 finished. The remaining queued request then fires, causing the
                 notifier in push() to be called out-of-scope = segfault.
        */
        const uint MAX_CONNS = 2;

        /// Total number of records to push
        const RECORD_COUNT = 100;

        /// DMQ client instance used to send records to fake DMQ
        private DmqClient dmqclient;

        /// Total count of successful Push requests
        private uint total_pushed;

        /// Count of consecutive failed Push requests. This is not expected to
        /// exceed the number of connections owned by the client. (When the fake
        /// node is shut down, all active requests -- at most one per connection
        /// -- will fail.)
        private uint consecutive_errors;

        override public void run ( )
        {
            Dmq.initialize();
            dmq.start("127.0.0.1");

            this.dmqclient = new DmqClient(theScheduler.epoll, MAX_CONNS);
            this.dmqclient.addNode("127.0.0.1".dup, dmq.node_item.Port);

            uint i;
            while ( this.total_pushed < RECORD_COUNT )
            {
                // Restart the node periodically. Doing so will cause all
                // in-progress requests to fail, incrementing
                // this.consecutive_errors.
                if ( i++ % 10 == 0 )
                {
                    // Check that the expected number of records are in the DMQ.
                    enforce!("==")(dmq.getSize("test").records, this.total_pushed);

                    // Restart the DMQ. The data should remain intact.
                    dmq.stop();
                    dmq.restart();

                    enforce!("==")(dmq.getSize("test").records, this.total_pushed);
                }

                // Push a record
                this.push();
                enforce!("<=")(this.consecutive_errors, MAX_CONNS);
            }

            dmq.stop();

            // FIXME: the fake node connections do not unregister their clients
            // when finalized. This line could be removed, if they did.
            theScheduler.shutdown();
        }

        /// Assigns a Push request and suspends the fiber until it finishes
        private void push ( )
        {
            bool finished;

            void notifier ( DmqClient.RequestNotification info )
            {
                if ( info.type == info.type.Finished )
                {
                    finished = true;

                    if ( info.succeeded )
                    {
                        this.consecutive_errors = 0;
                        this.total_pushed++;
                    }
                    else
                    {
                        this.consecutive_errors++;
                    }

                    if (this.suspended)
                        this.resume();
                }
            }

            char[] pushDg ( DmqClient.RequestContext context )
            {
                return "some value".dup;
            }

            this.dmqclient.assign(this.dmqclient.push("test", &pushDg, &notifier));

            if ( !finished )
                this.suspend();
        }
    }

    initScheduler(SchedulerConfiguration.init);
    theScheduler.schedule(new TestTask);
    theScheduler.eventLoop();
}
