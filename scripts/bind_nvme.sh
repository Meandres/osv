#!/usr/bin/env bash

driver=$1
nvme_id=$2

bind_nvme(){
    echo "1" > "/sys/bus/pci/devices/0000:$nvme_id/remove"
    sleep 2
    echo "1" > /sys/bus/pci/rescan
}

bind_vfio(){
    echo "vfio-pci" > "/sys/bus/pci/devices/0000:$nvme_id/driver_override"
    echo "0000:$nvme_id" > "/sys/bus/pci/devices/0000:$nvme_id/driver/unbind"
    echo "0000:$nvme_id" > "/sys/bus/pci/drivers_probe"
}

if [[ ! $nvme_id =~ ^(07:00.0|c3:00.0)$ ]]; then
    echo "Please specify the correct nvme_id (correct values for irene: c3:00.0 (PCI5) or 07:00.0 (PCI4))"
    exit 1
fi

if [[ ! $driver =~ ^(nvme|vfio)$ ]]; then
    echo "Please specify the correct driver (nvme or vfio)"
    exit 1
fi

if [[ $driver == "nvme" ]]; then
    bind_nvme
fi

if [[ $driver == "vfio" ]]; then
    bind_vfio
fi
