/******************************************************************************

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dmqtest.cases.Basic;

/******************************************************************************

    Imports

******************************************************************************/

import dmqtest.DmqTestCase;

import ocean.core.Test;

/******************************************************************************

    Most simple push/pop sanity test

******************************************************************************/

class Sanity : DmqTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.name = "Basic sanity push/pop";
        return desc;
    }

    override public void run ( )
    {
        auto payloads = [ "foo", "bar" ];

        foreach (payload; payloads)
            this.dmq.push(this.channels[0], payload);

        foreach (payload; payloads)
            test!("==")(payload, this.dmq.pop(this.channels[0]));
    }
}

/*******************************************************************************

    Basic sanity test that ensures that neo connection and authentication with
    tested DMQ node succeed. Other tests don't make any sense if this fails.

*******************************************************************************/

class NeoConnection : DmqTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.priority = 1000; // must run first
        desc.name = "Neo connect / authenticate";
        desc.fatal = true;
        return desc;
    }

    override public void run ( )
    {
        // dmq.neo.connect() is called in DmqTestCase.prepare
    }
}
