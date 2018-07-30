/*******************************************************************************

    Request resource acquirer.

    Via an instance of this interface, a request is able to acquire different
    types of resource which it requires during its lifetime.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.neo.request.core.IRequestResources;

public interface IRequestResources
{
    /***************************************************************************

        Returns:
            a pointer to a new chunk of memory (a void[]) to use during the
            request's lifetime

    ***************************************************************************/

    void[]* getVoidBuffer ( );
}
