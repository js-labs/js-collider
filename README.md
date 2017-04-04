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

[![Join the chat at https://gitter.im/js-labs/js-collider](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/js-labs/js-collider?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

JS-Collider is an asynchronous event-driven Java network (NIO)
application framework designed to provide maximum performance
and scalability for applications having not too many connections
but significant amount of network traffic (both incoming and outgoing).

Performance is achieved by specially designed threading model
and lock-free algorithms ([learn more](https://github.com/js-labs/js-collider/wiki/Performance%20benchmarks))

### Main features:

* simple and flexible API ([learn more](https://github.com/js-labs/js-collider/wiki/API))
* UDP (with multicast) support
* shared memory IPC support out-of-the-box ([learn more](https://github.com/js-labs/js-collider/wiki/Shared%20Memory%20IPC))
* no GC overhead on income data, only one allocation per output message
* plain Java 1.7 (no any unsafe cheating)

Refer the [Wiki](https://github.com/js-labs/js-collider/wiki)
for API documentation and performance tests results.

### Downloading from the Maven central repository

Add the following dependency section to your pom.xml:

    <dependencies>
      ...
        <dependency>
          <groupId>org.js-labs</groupId>
          <artifactId>js-collider</artifactId>
          <version>A.B.C</version>
          <scope>compile</scope>
        </dependency>
      ...
    </dependencies>

### Building

You will require JDK 1.7 and appache ant or maven.

    ant package

or

    mvn package
    
### Running tests

    ant tests

### Contacts

Need more features or support? Contact info@js-labs.org
