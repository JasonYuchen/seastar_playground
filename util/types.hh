//
// Created by jason on 2021/9/22.
//

#pragma once

#define DISALLOW_COPY_AND_ASSIGN(TypeName)          \
TypeName(const TypeName&) = delete;                 \
TypeName& operator=(const TypeName&) = delete

#define DISALLOW_COPY_MOVE_AND_ASSIGN(TypeName)     \
TypeName(const TypeName&) = delete;                 \
TypeName& operator=(const TypeName&) = delete;      \
TypeName(TypeName&&) = delete;                      \
TypeName& operator=(const TypeName&&) = delete

#define DEFAULT_COPY_AND_ASSIGN(TypeName)           \
TypeName(const TypeName&) = default;                \
TypeName& operator=(const TypeName&) = default

#define DEFAULT_MOVE_AND_ASSIGN(TypeName)           \
TypeName(TypeName&&) noexcept = default;            \
TypeName& operator=(TypeName&&) noexcept = default

#define DEFAULT_COPY_MOVE_AND_ASSIGN(TypeName)      \
TypeName(const TypeName&) = default;                \
TypeName& operator=(const TypeName&) = default;     \
TypeName(TypeName&&) noexcept = default;            \
TypeName& operator=(TypeName&&) noexcept = default

namespace rafter::util {

template<typename T>
struct type {};

}  // namespace rafter::util
