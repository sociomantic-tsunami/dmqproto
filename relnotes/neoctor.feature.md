### New `DmqClient` constructor accepting neo but not legacy configuration

`dmqproto.client.DmqClient`

A constructor has been added to `DmqClient` that accepts a `Neo.Config`
parameter while not needing the legacy `IClient.Config` configuration parameter.
Previously an application using only the neo protocol had to set up a useless
legacy nodes file just to pass it in the `IClient.Config` configuration
parameter, which now is not necessary any more.
