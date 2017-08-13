BTrDB C++ Bindings
==================

These are the C++ bindings for BTrDB version 4. The API is similar to the golang API, but no official documentation exists at the moment.

Currently, the build has been tested on Ubuntu 16.04. First, install gRPC and its dependencies by following the instructions at https://github.com/grpc/grpc. These bindings have been tested with gRPC 1.4.2. To build the code, execute `make`. To install the headers and object files, execute `sudo make install`. Once this is done, you can use the BTrDB C++ bindings in your C++ program by adding
```
#include <btrdb/btrdb.h>
```
to your C++ code. You will also need to compile and link with `-lgrpc++ -lgrpc -lbtrdb -lpthread -ldl`. An example command-line program, which demonstrates this, is provided in `examples/cmd`.

I have also successfully built this code on Windows 10 using the Microsoft Visual C++ Compiler.

License
-------
This code is licensed under the GNU Lesser General Public License, version 3 (LGPLv3).
