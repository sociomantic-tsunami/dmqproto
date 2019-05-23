/*******************************************************************************

    Parameters for a DMQ request.

    Copyright:
        Copyright (c) 2010-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.params.RequestParams;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.request.params.IChannelRequestParams;

import swarm.client.request.context.RequestContext;

import swarm.client.ClientCommandParams;

import swarm.client.connection.model.INodeConnectionPoolInfo;

import dmqproto.client.legacy.internal.request.params.IODelegates;

import dmqproto.client.legacy.internal.request.notifier.RequestNotification;

import dmqproto.client.legacy.DmqConst;

import dmqproto.client.legacy.internal.request.model.IRequest;

import swarm.client.request.model.ISuspendableRequest;

import dmqproto.client.legacy.internal.request.model.IProducer;

import ocean.core.SmartUnion;
import ocean.core.Traits;

import ocean.io.select.EpollSelectDispatcher;

import ocean.transition;


public class RequestParams : IChannelRequestParams
{
    /***************************************************************************

        Local type redefinitions

    ***************************************************************************/

    public alias .PutValueDg PutValueDg;
    public alias .GetValueDg GetValueDg;
    public alias .GetNodeValueDg GetNodeValueDg;
    public alias .GetNumConnectionsDg GetNumConnectionsDg;
    public alias .RegisterSuspendableDg RegisterSuspendableDg;
    public alias .RegisterStreamInfoDg RegisterStreamInfoDg;
    public alias .ProducerDg ProducerDg;


    /**************************************************************************

        Node identifier counter, required for requests that try other nodes on
        failure

     **************************************************************************/

    public struct NodeId
    {
        /**********************************************************************

            Counter start and current counter value

         **********************************************************************/

        public uint start, current;


        /**********************************************************************

            Sets the counter start and current counter value to the initial
            counter value c

            Params:
                c = initial counter value

            Returns:
                counter value

         **********************************************************************/

        public uint opAssign ( uint c )
        {
            this.start = c;
            this.current = c;

            return c;
        }


        /**********************************************************************

            Increases the current counter value by 1, wrapping around to 0 if
            the incremented counter value reaches end.

            Params:
                end = upper bound for wrapping around (counter value will
                      always be less than end)

            Returns:
                this instance

         **********************************************************************/

        public typeof(this) next ( uint end )
        {
            this.current = (this.current + 1) % end;

            return this;
        }


        /**********************************************************************

            Returns:
                true if the current counter value has reached the counter start
                value or false otherwise

         **********************************************************************/

        public bool finished ( )
        {
            return this.current == this.start;
        }
    }


    /***************************************************************************

        Request I/O delegate union

    ***************************************************************************/

    public union IODg
    {
        PutValueDg put_value;
        GetValueDg get_value;
        GetNodeValueDg get_node_value;
        GetNumConnectionsDg get_num_connections;
        ProducerDg producer;
    }

    public alias SmartUnion!(IODg) IOItemUnion;


    /***************************************************************************

        I/O delegates.

        Note that some requests require two I/O delegates (an input delegate and
        an output delegate, for example). Probably a dynamic array of delegates
        would be better, but this causes complications with other parts of the
        code (specifically the templates in swarm.client.request) which
        expect this to be a single value, not an array. As this code is anyway
        planned to be completely reworked at some point, for now, for the sake
        of simplicity, we just have two members.

    ***************************************************************************/

    public IOItemUnion io_item;

    public IOItemUnion io_item2;


    /***************************************************************************

        List of DMQ channels, for multi-channel requests (slice).

    ***************************************************************************/

    public Const!(char[])[] channels;

    /***************************************************************************

        Node identifier counter instance, required for requests that try other
        nodes on failure

    ***************************************************************************/

    public NodeId node_id;


    /**************************************************************************

        Suspendable request registration callback

     **************************************************************************/

    public RegisterSuspendableDg suspend_register;

    /**************************************************************************

        Suspendable request registration callback

     **************************************************************************/

    public RegisterSuspendableDg suspend_unregister;


    /***************************************************************************

        Delegate which receives an IStreamInfo interface when a stream request
        has just started.

    ***************************************************************************/

    public RegisterStreamInfoDg stream_info_register;


    /***************************************************************************

        false if this request may be reassigned to the node with the least
        amount of data in the request queue or true if it should be performed
        by the node it is assigned to.

        Request classes that use the reregistrator should set this flag to true
        to avoid an infinite request reassignment loop.

    ***************************************************************************/

    public bool force_assign;

    /***************************************************************************

        News a DMQ client RequestNotification instance and passes it to the
        provided delegate.

        Params:
            info_dg = delegate to receive IRequestNotification instance

    ***************************************************************************/

    override protected void notify_ ( scope void delegate ( IRequestNotification ) info_dg )
    {
        scope info = new RequestNotification(cast(DmqConst.Command.E)this.command,
            this.context);
        info_dg(info);
    }


    /***************************************************************************

        Copies the fields of this instance from another.

        All fields are copied by value. (i.e. all arrays are sliced.)

        Note that the copyFields template used by this method relies on the fact
        that all the class' fields are non-private. (See template documentation
        in ocean.core.Traits for further info.)

        Params:
            params = instance to copy fields from

    ***************************************************************************/

    override protected void copy__ ( IRequestParams params )
    {
        auto dmq_params = cast(RequestParams)params;
        this.tupleof[] = dmq_params.tupleof[];
    }


    /***************************************************************************

        Add the serialisation override methods

    ***************************************************************************/

    mixin Serialize!();
}
