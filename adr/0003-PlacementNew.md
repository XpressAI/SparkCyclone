# C++ Data Lifetime Management via Placement New

## Status
PROPOSED

Proposed by: Carlos Reyes (February 18, 2022)

Discussed with:

* Benson Ma (early ideas)
* Eduardo Gonzalez (overall design goals)

## Context
Our architecture has a low-level C/C++ layer and a high level Java/Scala layer. We use JavaCpp to communicate between
the layers. We would like to only use C++ code in the low level layer.

Right now we allocate return values using C++ and deallocate them from Scala.
This requires us to use C-style calloc/free system calls. We would like to migrate away from this design.

## Proposal
Eventually we want to move the return value deallocation to the same layer where the data is allocated,
which is the low level layer. The safest way to do this is to rely on C++ destructors to clean up the heap-allocated
objects. I view this proposal as an interim solution along the way to that goal.

The idea is to maintain the C malloc/free calls while layering on top the C++ new/delete semantics.
That is what the placement new syntax gives you. Please see the attached sample code.

A second step would be implementing deleter functions (see sample).
These could eventually be called from Scala to reclaim the storage for the objects.
Once this new deleting mechanism is in place and working, we can remove the cleanup code from the high level layer. 

### Sample Code
```
#include <iostream>                                                                                                                                                                                                                          
#include <cassert>                                                                                                                                                                                                                           
                                                                                                                                                                                                                                             
// note that the object itself knows nothing about how it was allocated                                                                                                                                                                      
// it still supports:   Data d(456, "there");                                                                                                                                                                                                
class Data                                                                                                                                                                                                                                   
{                                                                                                                                                                                                                                            
public:                                                                                                                                                                                                                                      
  int i_;                                                                                                                                                                                                                                    
  const char* s_;                                                                                                                                                                                                                            
                                                                                                                                                                                                                                             
  Data(int i, const char* s) : i_(i), s_(s) { std::cerr << "Data()" << std::endl; }                                                                                                                                                          
                                                                                                                                                                                                                                             
  ~Data() { i_ = 0; s_ = nullptr; std::cerr << "~Data()" << std::endl; }                                                                                                                                                                     
                                                                                                                                                                                                                                             
  void print() { std::cerr << i_ << "\t" << s_ << std::endl; }
  
  static deleter(Data* data) { delete data; }   // not used yet - for future                                                                                                                                                                               
};                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                             
int main()                                                                                                                                                                                                                                   
{                                                                                                                                                                                                                                            
  // allocate but do not initialize the data for the object itself                                                                                                                                                                           
  // similar:   byte pv[sizeof(Data)];                                                                                                                                                                                                       
  std::cerr << "before malloc" << std::endl;                                                                                                                                                                                                 
  void* pv = malloc(sizeof(Data));                                                                                                                                                                                                           
  // placement new uses the pointer to the buffer you provide                                                                                                                                                                                
  // it does not allocate any data                                                                                                                                                                                                           
  // note that the pointer to the data buffer should be void*, as expected                                                                                                                                                                   
  // equivalent:   new (malloc(sizeof(Data))) Data(123, "hello");                                                                                                                                                                            
  std::cerr << "before placement new" << std::endl;                                                                                                                                                                                          
  Data* pd = new (pv) Data(123, "hello");                                                                                                                                                                                                    
  assert(pd == pv);                                                                                                                                                                                                                          
  std::cerr << "before print()" << std::endl;                                                                                                                                                                                                
  pd->print();                                                                                                                                                                                                                               
  // you can call the destructor of an object directly in C++                                                                                                                                                                                
  // it behaves like a normal destructor, but the object itself is not deleted                                                                                                                                                               
  // saying "delete pd" would delete the object itself and would be an error                                                                                                                                                                 
  // calling the destructor before memory deallocation is not required, but good practice                                                                                                                                                    
  std::cerr << "before destructor" << std::endl;                                                                                                                                                                                             
  pd->~Data();                                                                                                                                                                                                                               
  assert(pd->i_ == 0 && pd->s_ == nullptr);                                                                                                                                                                                                  
  // the data for the object is still allocated                                                                                                                                                                                              
  // it needs to be freed, using the counterpart of malloc                                                                                                                                                                                   
  std::cerr << "before free" << std::endl;                                                                                                                                                                                                   
  free(pd);                                                                                                                                                                                                                                  
  std::cerr << "after free" << std::endl;                                                                                                                                                                                                    
}                                                                                                                                                                                                                                            
```

### Sample Code - Output
```
before malloc                                                                                                                                                                                                                                
before placement new                                                                                                                                                                                                                         
Data()                                                                                                                                                                                                                                       
before print()                                                                                                                                                                                                                               
123     hello                                                                                                                                                                                                                                
before destructor                                                                                                                                                                                                                            
~Data()                                                                                                                                                                                                                                      
before free                                                                                                                                                                                                                                  
after free                                                                                                                                                                                                                                   
```

## Consequences 
This is a stepping-stone towards have a low level layer fully in C++. It is designed not to disrupt the current system
and to accommodate a gradual adoption. 

### Advantages
The main idea is to start relying on C++ destructors to clean up the memory allocations.
  
### Disadvantages
The placement new syntax is not very well known or understood. Hopefully the sample code above helps.

Calling ```delete``` on an object that was allocated using placement new will potentially lead to program crashes.

## Discussion
< Questions/Answers and suggestions based on the above proposed items >
