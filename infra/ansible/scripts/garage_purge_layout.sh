#!/usr/bin/env bash
#
# Back up a Garage node's metadata + data directories and, with --purge-layout,
# delete its stored cluster layout.
#
# This exists for ONE job: changing the cluster's replication factor. That
# factor is stored in the cluster layout, so a node whose config disagrees with
# its stored layout refuses to start, and upstream's only supported path is to
# stop every node, purge the layout everywhere, and rebuild it:
#   https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#replication_factor
#   https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1173
# Run it by hand on EVERY node, with Garage stopped. See "Changing the
# replication factor" in DATA_SHARING.md for the surrounding procedure.
#
# The two directories are backed up differently, on purpose:
#
#   metadata_dir -> real copy (cp -a). db.sqlite is mutated in place, so a
#     hardlink would just alias the live file and protect nothing. It is small
#     (hundreds of MB). This is the backup that matters: cluster_layout lives
#     inside this directory, so the purge is undone by restoring one file.
#
#   data_dir -> hardlink copy (cp -rl). New directory entries pointing at the
#     same inodes: seconds instead of copying hundreds of GB, and ~zero extra
#     disk. Sound because Garage data blocks are immutable — written once,
#     never edited in place, and deleting one is just an unlink. If the
#     post-purge rebalance drops blocks from the live tree, this tree still
#     holds those inodes and the bytes survive.
#
#     Two limits, so nobody mistakes it for a real backup: it is ONE physical
#     copy, so it does nothing against disk failure or bit-rot; and hardlinks
#     cannot cross filesystems, so it must live on the same mount as data_dir
#     (asserted below).
#
# Usage: sudo ./garage_purge_layout.sh [--purge-layout] [--config /etc/garage.toml]

set -euo pipefail

CONFIG="/etc/garage.toml"
PURGE=0

while [[ $# -gt 0 ]]; do
	case "$1" in
	--purge-layout) PURGE=1 ;;
	--config)
		CONFIG="${2:?--config needs a path}"
		shift
		;;
	-h | --help)
		sed -n "2,34p" "$0"
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 2
		;;
	esac
	shift
done

die() {
	echo "ERROR: $*" >&2
	exit 1
}

[[ ${EUID} -eq 0 ]] || die "must run as root (the dirs are owned by garage)"
[[ -r ${CONFIG} ]] || die "cannot read ${CONFIG}"

# Refuse on a live node: copying a running SQLite metadata DB yields a backup
# that may not be consistent, and purging the layout underneath a running
# Garage is undefined.
if systemctl is-active --quiet garage; then
	die "garage is still running. Stop it on EVERY node first: systemctl stop garage"
fi

# Anchored so commented-out examples in the config are ignored.
toml_path() {
	grep -oP "^\s*$1\s*=\s*\"\K[^\"]+" "${CONFIG}" | head -1
}

META="$(toml_path metadata_dir)"
DATA="$(toml_path data_dir)"
[[ -d ${META} ]] || die "metadata_dir '${META}' from ${CONFIG} is not a directory"
[[ -d ${DATA} ]] || die "data_dir '${DATA}' from ${CONFIG} is not a directory"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
META_BAK="${META}.bak-${TS}"
DATA_BAK="${DATA}.bak-${TS}"
LAYOUT="${META}/cluster_layout"

[[ -e ${META_BAK} ]] && die "${META_BAK} already exists"
[[ -e ${DATA_BAK} ]] && die "${DATA_BAK} already exists"

# Hardlinks cannot cross filesystems; the backup sits beside data_dir, so the
# parent must be the same device.
if [[ "$(stat -c %d "${DATA}")" != "$(stat -c %d "$(dirname "${DATA}")")" ]]; then
	die "${DATA} is itself a mountpoint; hardlink backup would cross filesystems.
     Back it up by hand onto the same filesystem, or snapshot the volume."
fi

echo "=== ${HOSTNAME:-$(hostname)} ==="
echo "config      : ${CONFIG}"
echo "metadata_dir: ${META}  ($(du -sh "${META}" | cut -f1))"
echo "data_dir    : ${DATA}  ($(du -sh "${DATA}" | cut -f1))"
echo "layout file : ${LAYOUT}$([[ -e ${LAYOUT} ]] || echo '  (absent — already purged?)')"
echo
df -h "${META}" "${DATA}"
echo

echo ">>> copying metadata (real copy) -> ${META_BAK}"
cp -a "${META}" "${META_BAK}"

echo ">>> linking data (hardlinks, no bytes copied) -> ${DATA_BAK}"
cp -rl "${DATA}" "${DATA_BAK}"

echo
echo "backups in place:"
du -sh "${META_BAK}"
# --apparent-size would report the full tree; the real cost is the new inodes.
echo "${DATA_BAK} (hardlinks; shares blocks with ${DATA}, so 'used' below barely moves)"
df -h "${META}" "${DATA}"
echo

if [[ ${PURGE} -eq 1 ]]; then
	if [[ -e ${LAYOUT} ]]; then
		echo ">>> removing ${LAYOUT}"
		rm -f "${LAYOUT}"
	else
		echo ">>> ${LAYOUT} already absent, nothing to purge"
	fi
else
	echo "Backups only. To also purge the layout, re-run with --purge-layout"
	echo "(or: rm -f ${LAYOUT})"
fi

cat <<MSG

Next: repeat on every other node before starting any of them.

Undo the purge (per node, Garage stopped):
    cp -a ${META_BAK}/cluster_layout ${META}/cluster_layout

Full metadata rollback (per node, Garage stopped):
    rm -rf ${META} && cp -a ${META_BAK} ${META}

Once the new layout is applied, rebalanced and verified, drop the backups:
    rm -rf ${META_BAK} ${DATA_BAK}
MSG
