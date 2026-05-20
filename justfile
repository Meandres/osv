osv_dir := justfile_directory()
ssd_id := "c5:00.0"


build-kernel mode="release" app="benchmarks/example/example.o":
    #!/usr/bin/env bash
    set -euo pipefail
    cd {{osv_dir}}
    make -j$(nproc) mode={{mode}} APP_OBJECTS="{{app}}" bootfs_manifest=bootfs_empty.manifest.skel

# Boot OSv with the NVMe SSD (ssd_id) via PCI passthrough (VFIO).
# Uses PVH kernel mode (-k) to bypass BIOS INT 13h disk reads; without this
# SeaBIOS may assign the VFIO NVMe as drive 0x80 and boot16.S reads from the
# wrong disk (it hardcodes dl=0x80 instead of using the BIOS-provided drive number).
run:
    #!/usr/bin/env bash
    set -euo pipefail
    cd {{osv_dir}}
    ./scripts/run.py -k -i build/last/loader.img --pass-pci "0000:{{ssd_id}}"

# ── DuckDB TPC-H runner ───────────────────────────────────────────────────────

# Run the DuckDB TPC-H benchmark on OSv with the NVMe SSD (ssd_id).
# Parameters are forwarded as OSv environment variables (--env=KEY=VALUE).
#
# Examples:
#   just run-duckdb query=1                          # Q1, one pass
#   just run-duckdb query=6 repeat=3                 # Q6, three passes (warm cache)
#   just run-duckdb query=all                        # all 22 queries
#   just run-duckdb mem=16G cache=8G duckdb=6G query=1
#   just run-duckdb mem=32G cache=24G duckdb=20G query=all
#
# mem    — total physical memory given to the QEMU VM (default 8G)
# cache  — bytes for OSv uCache; K/M/G suffix accepted (default: 50% of mem)
# duckdb — DuckDB buffer pool limit; K/M/G suffix accepted (default: 80% of cache)
run-duckdb query="1" repeat="1" mem="8G" vcpus="4" cache="" duckdb="" evict_batch="" prefetch_batch="":
    #!/usr/bin/env bash
    set -euo pipefail
    cd {{osv_dir}}

    # Ensure the SSD is bound to the vfio kernel driver.
    current_driver=$(sudo driverctl list-devices | grep "{{ssd_id}}" | awk '{print $2}')
    if [ "${current_driver}" != "vfio-pci" ]; then
        echo "==> Binding 0000:{{ssd_id}} to vfio-pci driver ..."
        sudo driverctl set-override "0000:{{ssd_id}}" vfio-pci
        sleep 1
    fi

    osv_env="--mount-nvme-ext --env=TPCH_QUERY={{query}} --env=TPCH_REPEAT={{repeat}} --env=UCACHE_EVICT_BATCH={{evict_batch}} --env=UCACHE_PREFETCH_BATCH={{prefetch_batch}}"
    if [ -n "{{cache}}" ]; then
        osv_env="${osv_env} --env=UCACHE_MEM={{cache}}"
    fi
    if [ -n "{{duckdb}}" ]; then
        osv_env="${osv_env} --env=DUCKDB_MEM={{duckdb}}"
    fi

    # -k: PVH kernel mode — QEMU loads loader-stripped.elf directly, bypassing
    # BIOS INT 13h.  Without this, SeaBIOS may give the VFIO NVMe drive 0x80
    # and boot16.S (which hardcodes dl=0x80) reads from the wrong disk.
    # -i loader.img: still needed so run.py doesn't fall back to usr.img (which
    # we don't build); the file is presented as a virtio-blk but not used for boot.
    taskset -c 0-63 ./scripts/run.py -k -i build/last/loader.img \
        -m "{{mem}}" -c "{{vcpus}}" \
        -e "${osv_env}" \
        --pass-pci "0000:{{ssd_id}}"

# Build DuckDB as a static archive with LTO, then link it into the kernel.
#
# DuckDB is compiled with -flto -ffat-lto-objects so ld.bfd + the GCC LTO
# plugin can do a whole-program optimisation pass across DuckDB and the kernel.
#
# The app wrapper (duckdb_app.cc) is compiled by the OSv Makefile using the
# kernel's own CXXFLAGS, with the DuckDB include path appended via EXTRA_CXXFLAGS.
#
# Usage:
#   just build-duckdb            # release build with LTO
#   just build-duckdb lto=0      # disable LTO (faster rebuild for debugging)
#
build-duckdb mode="release" lto="1":
    #!/usr/bin/env bash
    set -euo pipefail
    cd {{osv_dir}}

    duckdb_src="{{osv_dir}}/benchmarks/duckdb/duckdb"
    duckdb_build="{{osv_dir}}/build/{{mode}}.x64/duckdb"
    duckdb_lib="${duckdb_build}/src/libduckdb_static.a"

    lto_flags=""
    if [ "{{lto}}" = "1" ]; then
        lto_flags="-flto -ffat-lto-objects"
    fi

    # --- Step 1: build DuckDB static library ---
    osv_bench_inc="{{osv_dir}}/benchmarks/duckdb"
    cmake -S "${duckdb_src}" \
          -B "${duckdb_build}" \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_C_COMPILER="${CC:-x86_64-linux-gnu-gcc}" \
          -DCMAKE_CXX_COMPILER="${CXX:-x86_64-linux-gnu-g++}" \
          -DCMAKE_C_FLAGS="-D__OSV__=1 -I${osv_bench_inc} -fno-pie -fno-stack-protector -mfma -ftls-model=local-exec ${lto_flags}" \
          -DCMAKE_CXX_FLAGS="-D__OSV__=1 -I${osv_bench_inc} -fno-pie -fno-stack-protector -mfma -ftls-model=local-exec ${lto_flags}" \
          -DBUILD_SHARED_LIBS=OFF \
          -DBUILD_SHELL=OFF \
          -DBUILD_UNITTESTS=OFF \
          -DDUCKDB_EXPLICIT_PLATFORM=linux_amd64 \
          -DENABLE_SANITIZER=OFF \
          -DENABLE_UBSAN=OFF \
          -DMUSL_ENABLED=1

    cmake --build "${duckdb_build}" \
        --target duckdb_static core_functions_extension parquet_extension duckdb_generated_extension_loader \
        --parallel $(nproc)

    # --- Step 2: build ext filesystem modules (lwext4 + libext) ---
    # lwext4 clones upstream on first run; subsequent runs are fast (already built).
    make -C modules/lwext4
    make mode={{mode}} generated-headers
    make -C modules/libext module ARCH=x64 OSV_BUILD_PATH="{{osv_dir}}/build/release.x64"

    # --- Step 3: build kernel with DuckDB and OSv uCache file system baked in ---
    core_fn_lib="${duckdb_build}/extension/core_functions/libcore_functions_extension.a"
    parquet_lib="${duckdb_build}/extension/parquet/libparquet_extension.a"
    ext_loader_lib="${duckdb_build}/extension/libduckdb_generated_extension_loader.a"

    make -j$(nproc) \
        mode={{mode}} \
        lto={{lto}} \
        APP_OBJECTS="benchmarks/duckdb/duckdb_app.o benchmarks/duckdb/osv_ucache_file_system.o" \
        APP_LIBS="${duckdb_lib} ${core_fn_lib} ${parquet_lib} ${ext_loader_lib}" \
        EXTRA_CXXFLAGS="-I${duckdb_src}/src/include -I${duckdb_src}/third_party/concurrentqueue -I${duckdb_src}/extension/parquet/include -I${duckdb_src}/third_party/thrift" \
        app_local_exec_tls_size=2048 \
        bootfs_manifest=bootfs_ext.manifest.skel

bind-ssd-vfio:
    #!/usr/bin/env bash
    current_driver=$(sudo driverctl list-devices | grep {{ssd_id}} | awk '{print $2 }')
    [ $current_driver = "vfio-pci" ] || sudo driverctl set-override 0000:{{ssd_id}} vfio-pci

bind-ssd-nvme:
    #!/usr/bin/env bash
    current_driver=$(sudo driverctl list-devices | grep {{ssd_id}} | awk '{print $2 }')
    if [ "$current_driver" != "nvme" ]
    then
        sudo driverctl set-override 0000:{{ssd_id}} nvme
    fi

# Run the DuckDB TPC-H benchmark directly on Linux (no VM).
# Ensures the SSD is bound to the nvme kernel driver and mounted at /nvme,
# then launches benchmarks/duckdb/duckdb_bench with the same env-var knobs
# as run-duckdb (OSv).
#
# Examples:
#   just run-duckdb-linux query=1
#   just run-duckdb-linux query=6 repeat=3 file_cache=4G threads=8
#   just run-duckdb-linux query=all duckdb=20G file_cache=8G threads=32
#
# threads    — total thread count including the main thread (default: nproc)
#              DuckDB worker threads = threads-1, main thread pinned to CPU threads-1
# duckdb     — DuckDB buffer pool limit; K/M/G suffix accepted (default: 40% of RAM)
# file_cache — DuckDB CachingFileSystem cap; K/M/G suffix (default: enabled, no cap)
#              pass 0 or "off" to disable
run-duckdb-linux query="1" repeat="1" threads="" file_cache="" duckdb="":
    #!/usr/bin/env bash
    set -euo pipefail

    bench_bin="{{osv_dir}}/benchmarks/duckdb/duckdb_bench"
    if [ ! -x "${bench_bin}" ]; then
        echo "Error: ${bench_bin} not found — run 'make linux' in benchmarks/duckdb/ first" >&2
        exit 1
    fi

    # Ensure the SSD is bound to the nvme kernel driver.
    current_driver=$(sudo driverctl list-devices | grep "{{ssd_id}}" | awk '{print $2}')
    if [ "${current_driver}" != "nvme" ]; then
        echo "==> Binding 0000:{{ssd_id}} to nvme driver ..."
        sudo driverctl set-override "0000:{{ssd_id}}" nvme
        sleep 1
    fi

    # Discover the block device exposed by the NVMe controller.
    nvme_dev="/dev/$(ls /sys/bus/pci/devices/0000:{{ssd_id}}/nvme)n1"
    echo "==> NVMe block device: ${nvme_dev}"

    # Mount at /nvme if not already mounted.
    if ! mountpoint -q /nvme; then
        sudo mkdir -p /nvme
        sudo mount -o ro "${nvme_dev}" /nvme
        echo "==> Mounted ${nvme_dev} at /nvme (read-only)"
    else
        echo "==> /nvme already mounted"
    fi

    # Build the env for duckdb_bench.
    export TPCH_QUERY="{{query}}"
    export TPCH_REPEAT="{{repeat}}"
    if [ -n "{{duckdb}}" ]; then
        export DUCKDB_MEM="{{duckdb}}"
    fi
    if [ -n "{{file_cache}}" ]; then
        export DUCKDB_FILE_CACHE="{{file_cache}}"
    fi

    nthreads="{{threads}}"
    if [ -z "${nthreads}" ]; then
        nthreads="$(nproc)"
    fi

    echo "==> Running DuckDB TPC-H (TPCH_QUERY={{query}} TPCH_REPEAT={{repeat}} threads=${nthreads})"
    taskset -c 0-63 "${bench_bin}" "${nthreads}"

# ── NVMe setup ────────────────────────────────────────────────────────────────

# Format the NVMe SSD (ssd_id) as ext4, copy TPC-H Parquet files, then bind
# it to vfio-pci ready for PCI passthrough. WARNING: destroys all data on the SSD.
# Requires: driverctl, sudo.
setup-nvme-ssd tpch="tpch10" confirm="yes":
    #!/usr/bin/env bash
    set -euo pipefail

    src="/scratch/ilya/{{tpch}}"

    if [[ ! -d "${src}" ]]; then
        echo "Error: source directory '${src}' not found" >&2; exit 1
    fi
    if [[ ! -d "/sys/bus/pci/devices/0000:{{ssd_id}}" ]]; then
        echo "Error: PCI device '0000:{{ssd_id}}' not found" >&2; exit 1
    fi

    if [[ "{{confirm}}" == "yes" ]]; then
        read -p "WARNING: ALL DATA ON 0000:{{ssd_id}} WILL BE DESTROYED. Continue? (y/N) " -n 1 -r
        echo
        [[ $REPLY =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
    fi

    # Ensure the device is bound to the nvme driver so we can reach the block device.
    current_driver=$(sudo driverctl list-devices | grep "{{ssd_id}}" | awk '{print $2}')
    if [[ "${current_driver}" != "nvme" ]]; then
        echo "==> Binding 0000:{{ssd_id}} to nvme driver ..."
        sudo driverctl set-override "0000:{{ssd_id}}" nvme
        sleep 1
    fi

    nvme_dev="/dev/$(ls /sys/bus/pci/devices/0000:{{ssd_id}}/nvme)n1"
    echo "==> Block device: ${nvme_dev}"

    echo "==> Formatting ${nvme_dev} as ext4 ..."
    sudo mke2fs -F -t ext4 -O ^metadata_csum "${nvme_dev}"

    mnt=$(mktemp -d)
    cleanup() { sudo umount "${mnt}" 2>/dev/null || true; rmdir "${mnt}" 2>/dev/null || true; }
    trap cleanup EXIT

    echo "==> Copying Parquet files from ${src} ..."
    sudo mount "${nvme_dev}" "${mnt}"
    sudo mkdir -p "${mnt}/tpch"
    sudo cp -v "${src}"/*.parquet "${mnt}/tpch/"
    sudo umount "${mnt}"
    rmdir "${mnt}"
    trap - EXIT

    echo "==> Binding 0000:{{ssd_id}} to vfio-pci ..."
    sudo driverctl set-override "0000:{{ssd_id}}" vfio-pci

    echo "Done. Run with: just run"