Description
===========

``dmqproto`` is a library that contains the protocol for the Distributed Message
Queue (DMQ), including:

* The DMQ client (``src.dmqproto.client``).
* Base classes for the protocol handling parts of the DMQ node
  (``src.dmqproto.node``).
* A simple, "fake" DMQ node, for use in tests (``src.fakedmq``).
* A turtle env extension (``src.turtle.env.Dht``) providing a fake DMQ node for
  use in tests, including methods to inspect and modify its contents.
* A thorough test of the DMQ protocol, using the client to connect to a node.
  The test is run, in this repo, on a fake node, but it can be reused in other
  repos to test real node implementations. (``src.dmqtest``)

Dependencies
============

Dependency | Version
-----------|---------
ocean      | v4.0.x
swarm      | v5.0.x
makd       | v2.1.3
turtle     | v9.0.x

Versioning
==========

dmqproto's versioning follows `Neptune
<https://github.com/sociomantic-tsunami/neptune/blob/master/doc/library-user.rst>`_.

This means that the major version is increased for breaking changes, the minor
version is increased for feature releases, and the patch version is increased
for bug fixes that don't cause breaking changes.

Support Guarantees
------------------

* Major branch development period: 6 months
* Maintained minor versions: 1 most recent

Maintained Major Branches
-------------------------

======= ==================== =============== =====
Major   Initial release date Supported until Notes
======= ==================== =============== =====
v13.x.x v13.0.0_: 21/08/2017  TBD             First open source release
======= ==================== =============== =====

.. _v13.0.0: https://github.com/sociomantic-tsunami/dmqproto/releases/tag/v13.0.0
