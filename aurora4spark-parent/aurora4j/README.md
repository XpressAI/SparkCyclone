Aurora4j (WIP)
----------------------------
Aurora4j contains the minimal bindings to interact with the NEC vector engine.

The main function is to ensure data gets sent back and forth to the vector engine
from java in an efficient manner.

This happens through the usage of javacpp's [Pointer](http://bytedeco.org/javacpp/apidocs/org/bytedeco/javacpp/Pointer.html)
Javacpp, via its maven plugin parses the [AuroraPresets](./src/main/java/com/nec/aurora/AuroraPresets.java)
for headers and generates the necessary java native interface bindings
to interact with the vector engine based on the specified headers
and linked libraries. Javacpp will handle generation of the necessary jni c++
file and compile/link it to the necessary libraries based on what is specified.

The main entry point to this project that leverages the generated Aurora class is the
[AuroraOps](./src/main/java/com/nec/aurora/AuroraOps.java)

This will handle device interaction such as setting which device to allocate memory on,
allocating and freeing host/device memory and calling functions.

Everything is loosely typed here so it's a bit dangerous to use directly.
We will be adding a high level SDK based on the project's requirements
that's tested with a good set of sane defaults to ensure things like crashes
don't happen as often.


### Basic workflow

1. Compile executable using ncc

2. Create a process handle loading the executable

3. Create a context 

4. Allocate arguments

5. Call the function by name

5. Wait for the result

6. Free memory

A basic example that can be replicated using aurora4j can be found [here](https://www.hpc.nec/documents/veos/en/veoffload/md_GettingStarted.html)

