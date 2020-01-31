/*******************************************************************************

    Mixins for request setup structs used in DmqClient.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.RequestSetup;



/*******************************************************************************

    Imports

    Note that swarm.client.RequestSetup is imported publicly, as all of the
    templates it contains are needed wherever this module is imported.

*******************************************************************************/

public import swarm.client.RequestSetup;

import ocean.meta.types.Qualifiers;

/*******************************************************************************

    Mixin for the methods use by DMQ client requests which have an I/O
    delegate. One member field named `io_item` will be generated.

    Template Params:
        name = the name of the delegate field in IOItemUnion that will be set by
            this request.

*******************************************************************************/

public template IODelegate ( istring name )
{
    import ocean.meta.types.Qualifiers;
    import ocean.core.Verify;
    import ocean.core.TypeConvert : downcast;

    alias typeof(this) This;
    static assert (is(This == struct));

    /***************************************************************************

        Stored delegate + convenience aliases

        Not intended to be used directly outside of this mixin

    ***************************************************************************/

    private RequestParams.IOItemUnion io_item;

    mixin (`private alias typeof(io_item.` ~ name ~ `()) Delegate;`);
    static assert (is(Delegate == delegate));

    /***************************************************************************

        Sets the I/O delegate for a request.

        Params:
            io = I/O delegate

        Returns:
            this pointer for method chaining

    ***************************************************************************/

    public This* io ( Delegate io )
    {
        mixin(`this.io_item.` ~ name ~ `(io);`);

        return &this;
    }


    /***************************************************************************

        Copies the value of the io_item member into the provided
        request params class instance.

        Params:
            params = IRequestParams instance to write into

    ***************************************************************************/

    private void setup_io_item ( IRequestParams params )
    {
        auto params_ = downcast!(RequestParams)(params);
        verify(params_ !is null);

        params_.io_item = this.io_item;
    }

    /**************************************************************************/

    version ( unittest )
    {
        import dmqproto.client.legacy.DmqConst;
        import ocean.core.Test;
    }
    unittest
    {
        /*
         * Test if
         *   1. the io() method sets up this.io_item properly,
         *   2. the setup_io_item() method sets up RequestParams.io_item
         *      properly according to this.io_item.
         *
         * To test if io_item is set up properly we have to set up the struct of
         * parameters where this template is mixed in with a valid command, or
         * a struct invariant will fail. We use the fact that by convention the
         * command enum is identical with the struct name.
         */
        mixin("const command = DmqConst.Command.E." ~ This.stringof ~ ";");
        auto params = *This(command, null).io(null);
        mixin("const expected = params.io_item.active." ~ name ~ ";");
        test!("==")(params.io_item.active, expected);
        scope rp = new RequestParams;
        params.setup(rp);
        test!("==")(rp.io_item.active, expected);
    }
}

/*******************************************************************************

    Mixin for the methods use by DMQ client requests which have a second I/O
    delegate. One member field named `io_item2` will be generated.

    Template Params:
        name = the name of the delegate field in IOItemUnion that will be set by
            this request.

*******************************************************************************/

public template IODelegate2 ( istring name )
{
    import ocean.meta.types.Qualifiers;
    import ocean.core.TypeConvert : downcast;

    alias typeof(this) This;
    static assert (is(This == struct));

    /***************************************************************************

        Stored delegate + convenience aliases

        Not intended to be used directly outside of this mixin

    ***************************************************************************/

    private RequestParams.IOItemUnion io_item2;
    mixin (`private alias typeof(io_item2.` ~ name ~ `()) Delegate;`);
    static assert (is(Delegate == delegate));

    /***************************************************************************

        Sets the second I/O delegate for a request.

        Params:
            io = second I/O delegate

        Returns:
            this pointer for method chaining

    ***************************************************************************/

    public This* io2 ( Delegate io )
    {
        mixin(`this.io_item2.` ~ name ~ `(io);`);

        return &this;
    }


    /***************************************************************************

        Copies the value of the io_item2 member into the provided
        request params class instance.

        Params:
            params = IRequestParams instance to write into

    ***************************************************************************/

    private void setup_io_item2 ( IRequestParams params )
    {
        auto params_ = downcast!(RequestParams)(params);
        verify(params_ !is null);

        params_.io_item2 = this.io_item2;
    }

    /**************************************************************************/

    version ( unittest )
    {
        import dmqproto.client.legacy.DmqConst;
        import ocean.core.Test;
    }
    unittest
    {
        /*
         * Test if
         *   1. the io2() method sets up this.io_item2 properly,
         *   2. the setup_io_item2() method sets up RequestParams.io_item2
         *      properly according to this.io_item2.
         *
         * To test if io_item is set up properly we have to set up the struct of
         * parameters where this template is mixed in with a valid command, or
         * a struct invariant will fail. We use the fact that by convention the
         * command enum is identical with the struct name.
         */
        mixin("const command = DmqConst.Command.E." ~ This.stringof ~ ";");
        auto params = *This(command, null).io2(null);
        mixin("const expected = params.io_item.active." ~ name ~ ";");
        test!("==")(params.io_item2.active, expected);
        scope rp = new RequestParams;
        params.setup(rp);
        test!("==")(rp.io_item2.active, expected);
    }
}


/*******************************************************************************

    Mixin for the methods used by DMQ client requests which operate over
    multiple channels.

*******************************************************************************/

public template Channels ( )
{
    import ocean.core.TypeConvert : downcast;
    import ocean.core.Verify;
    import ocean.meta.types.Qualifiers;

    alias typeof(this) This;
    static assert (is(This == struct));

    /***************************************************************************

        Channels request operates on

    ***************************************************************************/

    private const(char[])[] channel_names;

    /***************************************************************************

        Sets the channels set for a request.

        Params:
            channels = list of channels for request

        Returns:
            this pointer for method chaining

    ***************************************************************************/

    public This* channels ( const(char[])[] channels )
    {
        verify(!!channels.length, "multi-channel request: empty list of channels");

        this.channel_names = channels;

        return &this;
    }


    /***************************************************************************

        Copies the value of the channel_names member into the provided
        request params class instance.

        Params:
            params = IRequestParams instance to write into

    ***************************************************************************/

    private void setup_channel_names ( IRequestParams params )
    {
        auto params_ = downcast!(RequestParams)(params);
        verify(params_ !is null);

        params_.channels = this.channel_names;
    }
}

