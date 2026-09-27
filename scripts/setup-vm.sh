#!/usr/bin/env bash
# Prepare an Ubuntu 24.04 VM as the Docker host for the benchmark. Idempotent.
# Usage: bash scripts/setup-vm.sh [--check-only]
set -euo pipefail

readonly EXPECTED_OS_ID="ubuntu"
readonly EXPECTED_OS_VERSION="24.04"
readonly PYTHON_VERSION="3.13"
readonly UDP_BUFFER_BYTES=8388608 # 8 MiB
readonly SYSCTL_FILE="/etc/sysctl.d/90-grpc-bench.conf"
readonly MODULES_FILE="/etc/modules-load.d/grpc-bench.conf"
readonly DOCKER_KEYRING="/etc/apt/keyrings/docker.asc"
readonly DOCKER_SOURCES="/etc/apt/sources.list.d/docker.list"
readonly NETEM_TEST_IMAGE="alpine:3.20"
readonly NETEM_TEST_CONTAINER="grpc-bench-netem-check"
readonly MIN_CPUS=4
readonly MIN_MEM_MIB=7500

check_only=0
relogin_needed=0
declare -a results=()

log() { printf '\n==> %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
warn() { printf '    WARN: %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
record() { results+=("$1|$2|$3"); } # status|check|detail

usage() {
    cat <<EOF
Usage: bash $(basename "$0") [--check-only]

Installs and configures: base packages, Docker Engine + compose plugin,
sch_netem kernel module, UDP buffer limits, CPU governor (if available),
uv and Python ${PYTHON_VERSION}. Then verifies the environment.

Options:
  --check-only   only run the verification step
  -h, --help     show this help
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --check-only) check_only=1 ;;
            -h | --help) usage; exit 0 ;;
            *) usage >&2; die "unknown argument: $1" ;;
        esac
        shift
    done
}

# ------------------------------------------------------------------ preflight

preflight() {
    [[ ${EUID} -ne 0 ]] || die "run as a regular user with sudo privileges, not as root"
    command -v sudo >/dev/null 2>&1 || die "sudo not found"
    [[ -r /etc/os-release ]] || die "/etc/os-release not found"

    # shellcheck source=/dev/null
    . /etc/os-release
    if [[ "${ID:-}" != "${EXPECTED_OS_ID}" || "${VERSION_ID:-}" != "${EXPECTED_OS_VERSION}" ]]; then
        die "expected Ubuntu ${EXPECTED_OS_VERSION}, found ${PRETTY_NAME:-unknown}"
    fi

    sudo -v || die "sudo authentication failed"
}

# ------------------------------------------------------------------ setup steps

install_base_packages() {
    log "Installing base packages"
    sudo apt-get update -y -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
        ca-certificates curl git make iproute2 ethtool openssl util-linux
}

install_docker() {
    log "Installing Docker Engine"

    if command -v snap >/dev/null 2>&1 && snap list docker >/dev/null 2>&1; then
        die "Docker snap is installed; remove it first: sudo snap remove docker"
    fi

    if command -v docker >/dev/null 2>&1 && sudo docker compose version >/dev/null 2>&1; then
        info "already installed: $(sudo docker --version)"
    else
        local codename arch
        # shellcheck source=/dev/null
        codename="$(. /etc/os-release && echo "${UBUNTU_CODENAME:-${VERSION_CODENAME}}")"
        arch="$(dpkg --print-architecture)"

        sudo install -m 0755 -d "$(dirname "${DOCKER_KEYRING}")"
        sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o "${DOCKER_KEYRING}"
        sudo chmod a+r "${DOCKER_KEYRING}"
        echo "deb [arch=${arch} signed-by=${DOCKER_KEYRING}] https://download.docker.com/linux/ubuntu ${codename} stable" \
            | sudo tee "${DOCKER_SOURCES}" >/dev/null

        sudo apt-get update -y -qq
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
            docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        info "installed: $(sudo docker --version)"
    fi

    sudo systemctl enable --now docker >/dev/null

    if ! id -nG "${USER}" | tr ' ' '\n' | grep -qx docker; then
        sudo usermod -aG docker "${USER}"
        relogin_needed=1
        info "added ${USER} to the docker group"
    fi
}

ensure_netem() {
    log "Enabling sch_netem kernel module"

    if ! sudo modprobe sch_netem 2>/dev/null; then
        local extra_pkg
        extra_pkg="linux-modules-extra-$(uname -r)"
        info "modprobe failed; installing ${extra_pkg}"
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${extra_pkg}" \
            || die "could not install ${extra_pkg}"
        sudo modprobe sch_netem || die "sch_netem still unavailable after installing ${extra_pkg}"
    fi

    echo "sch_netem" | sudo tee "${MODULES_FILE}" >/dev/null
    info "loaded and persisted in ${MODULES_FILE}"
}

configure_sysctl() {
    log "Configuring UDP socket buffer limits"

    local desired
    desired="$(cat <<EOF
# UDP socket buffer limits for QUIC (aioquic) under load.
net.core.rmem_max = ${UDP_BUFFER_BYTES}
net.core.wmem_max = ${UDP_BUFFER_BYTES}
EOF
)"
    if [[ -f "${SYSCTL_FILE}" ]] && [[ "$(cat "${SYSCTL_FILE}")" == "${desired}" ]]; then
        info "already configured"
    else
        printf '%s\n' "${desired}" | sudo tee "${SYSCTL_FILE}" >/dev/null
        info "written ${SYSCTL_FILE}"
    fi
    sudo sysctl -q -p "${SYSCTL_FILE}"
}

configure_cpu_governor() {
    log "Configuring CPU frequency governor"

    local governors=(/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor)
    if [[ ! -e "${governors[0]}" ]]; then
        info "cpufreq not exposed (typical on Hyper-V); skipping"
        return
    fi

    local gov_file
    for gov_file in "${governors[@]}"; do
        echo performance | sudo tee "${gov_file}" >/dev/null || warn "could not set ${gov_file}"
    done
    info "set to 'performance' (not persistent; rerun after reboot)"
}

install_uv_and_python() {
    log "Installing uv and Python ${PYTHON_VERSION}"

    export PATH="${HOME}/.local/bin:${PATH}"
    if command -v uv >/dev/null 2>&1; then
        info "uv already installed: $(uv --version)"
    else
        curl -LsSf https://astral.sh/uv/install.sh | sh
        command -v uv >/dev/null 2>&1 || die "uv not found after installation"
        info "installed: $(uv --version)"
    fi

    uv python install "${PYTHON_VERSION}"
}

# ------------------------------------------------------------------ verification

check_netem_in_container() {
    local pid output
    sudo docker rm -f "${NETEM_TEST_CONTAINER}" >/dev/null 2>&1 || true

    if ! sudo docker run -d --rm --name "${NETEM_TEST_CONTAINER}" "${NETEM_TEST_IMAGE}" sleep 60 >/dev/null 2>&1; then
        record FAIL "netem in container" "could not start ${NETEM_TEST_IMAGE}"
        return
    fi

    pid="$(sudo docker inspect -f '{{.State.Pid}}' "${NETEM_TEST_CONTAINER}")"
    if sudo nsenter -t "${pid}" -n tc qdisc add dev eth0 root netem loss 1% 2>/dev/null; then
        output="$(sudo nsenter -t "${pid}" -n tc qdisc show dev eth0 | head -1)"
        record OK "netem in container" "${output}"
    else
        record FAIL "netem in container" "tc qdisc add netem failed"
    fi

    sudo docker rm -f "${NETEM_TEST_CONTAINER}" >/dev/null 2>&1 || true
}

verify() {
    log "Verifying environment"
    export PATH="${HOME}/.local/bin:${PATH}"

    if sudo docker info >/dev/null 2>&1; then
        record OK "docker engine" "$(sudo docker version --format '{{.Server.Version}}')"
    else
        record FAIL "docker engine" "daemon not reachable"
    fi

    if sudo docker compose version >/dev/null 2>&1; then
        record OK "docker compose" "$(sudo docker compose version --short)"
    else
        record FAIL "docker compose" "plugin missing"
    fi

    if lsmod | grep -q '^sch_netem'; then
        record OK "sch_netem module" "loaded"
    else
        record FAIL "sch_netem module" "not loaded"
    fi

    if sudo docker info >/dev/null 2>&1; then
        check_netem_in_container
    else
        record FAIL "netem in container" "skipped: docker unavailable"
    fi

    local rmem wmem
    rmem="$(sysctl -n net.core.rmem_max)"
    wmem="$(sysctl -n net.core.wmem_max)"
    if (( rmem >= UDP_BUFFER_BYTES && wmem >= UDP_BUFFER_BYTES )); then
        record OK "udp buffers" "rmem_max=${rmem} wmem_max=${wmem}"
    else
        record FAIL "udp buffers" "rmem_max=${rmem} wmem_max=${wmem} (< ${UDP_BUFFER_BYTES})"
    fi

    if command -v uv >/dev/null 2>&1 && uv python find "${PYTHON_VERSION}" >/dev/null 2>&1; then
        record OK "uv + python" "$(uv --version), python $(uv python find "${PYTHON_VERSION}")"
    else
        record FAIL "uv + python" "uv or Python ${PYTHON_VERSION} missing"
    fi

    local cpus mem_mib
    cpus="$(nproc)"
    mem_mib="$(awk '/MemTotal/ {printf "%d", $2 / 1024}' /proc/meminfo)"
    if (( cpus >= MIN_CPUS )); then
        record OK "cpus" "${cpus}"
    else
        record WARN "cpus" "${cpus} (recommended >= ${MIN_CPUS})"
    fi
    if (( mem_mib >= MIN_MEM_MIB )); then
        record OK "memory" "${mem_mib} MiB"
    else
        record WARN "memory" "${mem_mib} MiB (recommended >= 8 GB)"
    fi
}

print_summary() {
    log "Summary"
    local entry status name detail failures=0
    for entry in "${results[@]}"; do
        IFS='|' read -r status name detail <<<"${entry}"
        printf '    [%-4s] %-20s %s\n' "${status}" "${name}" "${detail}"
        [[ "${status}" != "FAIL" ]] || failures=$((failures + 1))
    done

    if (( relogin_needed )); then
        printf '\n    Log out and back in (or run: newgrp docker) to use docker without sudo.\n'
    fi

    if (( failures > 0 )); then
        printf '\n    %d check(s) failed.\n' "${failures}"
        return 1
    fi
    printf '\n    Environment ready.\n'
}

main() {
    parse_args "$@"
    preflight

    if (( ! check_only )); then
        install_base_packages
        install_docker
        ensure_netem
        configure_sysctl
        configure_cpu_governor
        install_uv_and_python
    fi

    verify
    print_summary
}

main "$@"
