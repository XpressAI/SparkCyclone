/*
 * Copyright (c) 2022 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#pragma once

#include <functional>
#include <type_traits>

namespace cyclone::func {
  namespace {
    template <typename...>
    using void_t = void;

    template <class T, class R = void, class = void>
    struct is_callable : std::false_type { };

    template <class T>
    struct is_callable<T, void, void_t<std::result_of_t<T>>> : std::true_type { };

    template <class T, class R>
    struct is_callable<T, R, void_t<std::result_of_t<T>>> : std::is_convertible<std::result_of_t<T>, R> { };
  }

  /*
    This is a lightweight alternative to std::function that compiles down to
    much less lines of assembly instructions than std::function.  Note that it
    is intended to be best used as a function parameter and not as a value
    holder, since it is an rvalue reference.

    Copied from:
      https://vittorioromeo.info/index/blog/passing_functions_to_functions.html

    Extra notes:
      https://www.foonathan.net/2017/01/function-ref-implementation/
      https://probablydance.com/2013/01/13/a-faster-implementation-of-stdfunction/
  */

  template <typename TSignature>
  class function_view;

  template <typename TReturn, typename... TArgs>
  class function_view<TReturn(TArgs...)> final {
    private:
      using signature_type = TReturn(void*, TArgs...);
      // Memory location of the callable object (type-erased)
      void* _ptr;
      // Function pointer with the right type information that is able to
      // invoke the callable object
      TReturn (*_erased_fn)(void*, TArgs...);

    public:
      // Only allow construction of function_views from callable objects
      template <typename T, typename = std::enable_if_t<is_callable<T&(TArgs...)>{} &&
                                                       !std::is_same<std::decay_t<T>, function_view>{}>>
      // Save the pointer in the constructor
      function_view(T&& x) noexcept : _ptr{(void*)std::addressof(x)} {
        // Construct the function pointer
        _erased_fn = [](void* ptr, TArgs... xs) -> TReturn {
          return (*reinterpret_cast<std::add_pointer_t<T>>(ptr))(std::forward<TArgs>(xs)...);
        };
      }

      decltype(auto) operator()(TArgs... xs) const
        noexcept(noexcept(_erased_fn(_ptr, std::forward<TArgs>(xs)...))) {
        // Invoke the function pointer with the callable as the first arugment
        // applied to the forwarded arguments
        return _erased_fn(_ptr, std::forward<TArgs>(xs)...);
      }
  };
}
