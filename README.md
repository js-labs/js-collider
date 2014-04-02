                     JS-Collider
                     ===========

                       +-----+
                 /-----|     |-----\        +---+ 
                /      |     |      <=======|A/C| Session emitters
               /   /---|     |---\   \      +---+ (acceptor/connector)
              /   /    +-----+    \   \
              |   |     <----     |   |
    TCP/IP ---+-S |               |   |
    session  +-----+             +-----+
             |     |             |     |
             |     |             |     |
             |     |             |     |
             +-----+             +-----+
              |   |               |   |
              |   |     ---->     |   |
              \   \    +-----+    /   /
               \   \---|     |---/   /
                \      |     |  S   /
                 \-----|     |--+--/
                       +-----+  |
                                |
                              TCP/IP
                              session


JS-Collider is Java network (NIO) application framework designed
to provide maximum performance and scalability for applications
having not too many connections but significant amount of network
traffic (both incoming and outgoing).

### Main features:

* performance and scalability ([learn more](https://github.com/js-labs/js-collider/wiki/Performance))
* simple and flexible API ([learn more](https://github.com/js-labs/js-collider/wiki/API))
* UDP (with multicast) support
* shared memory IPC support out-of-the-box ([learn more](https://github.com/js-labs/js-collider/wiki/Shared Memory IPC))
* plain Java 1.7 (no any unsafe cheating)

Refer the [Wiki](https://github.com/js-labs/js-collider/wiki)
for API documentation and performance tests results.

### Building

You will require JDK 1.7 and appache ant.

    ant dist

### Running tests

    ant tests
    
### Contacts

Need more features or support? Contact info@js-labs.org
