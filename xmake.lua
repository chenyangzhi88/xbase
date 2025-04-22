includes("src/test/instruction_test.h")
set_project("XBase")
set_version("1.0.0")

add_rules("mode.debug", "mode.release")

local common_includes = {
    "src",
    "thirdparty/",
}

local common_cxflags_std = {"-std=c++20"}
local common_cxflags_coro = {"-fcoroutines"}
local common_cxflags_debug = {"-g"}
local common_cxflags_opt = {"-mavx2", "-msse4", "-msha"}
local common_links = {"pthread", "gtest"}

function set_common_configs(target)
    target:add("includedirs", common_includes)
    target:add("cxflags", common_cxflags_std, common_cxflags_coro)

    if is_mode("debug") then
        target:add("cxflags", debug_cxflags)
    end
    target:add("cxflags", common_cxflags_opt)
    target:add("links", common_links)
end

package("concurrentqueue")
    add_deps("cmake")
    set_sourcedir(path.join(os.scriptdir(), "thirdparty", "concurrentqueue"))
    on_install(function (package)
        local configs = {}
        table.insert(configs, "-DCMAKE_BUILD_TYPE=" .. (package:debug() and "Debug" or "Release"))
        import("package.tools.cmake").install(package, configs)
    end)
package_end()
add_requires("concurrentqueue")

package("libcoro")
    add_deps("cmake")
    set_sourcedir(path.join(os.scriptdir(), "thirdparty", "libcoro"))
    add_defines("LIBCORO_FEATURE_NETWORKING=ON")
    on_install(function (package)
        local configs = {}
        table.insert(configs, "-DCMAKE_BUILD_TYPE=" .. (package:debug() and "Debug" or "Release"))
        package:add("includedirs", "include")
        print("Package install dir: ", package:installdir())
        import("package.tools.cmake").install(package, configs)
    end)
package_end()
add_requires("libcoro")


target("dblib")
    set_kind("static")
    on_load(set_common_configs)
    add_files("src/**/*.cc")
    add_cxflags(common_cxflags_opt, common_cxflags_coro)
    add_packages("concurrentqueue", "libcoro")
    add_ldflags("-lxxhash", {force = true})
target_end()

target("rangedb")
    set_kind("binary")
    add_deps("dblib")
    on_load(set_common_configs)
    add_packages("concurrentqueue", "libcoro")
    add_files("src/*.cc")
target_end()

target("echo_server")
    set_kind("binary")
    add_deps("dblib")
    on_load(set_common_configs)
    add_packages("libcoro")
    add_files("demo/*.cc")
target_end()

--unit test
for _, file in ipairs(os.files("tests/*.cc")) do
    local name = path.basename(file)
    target(name)
        set_kind("binary")
        add_deps("dblib")
        add_packages("libcoro")
        on_load(set_common_configs)
        --set_default(false)
        add_files(file)
        add_tests("default")
    target_end()
end