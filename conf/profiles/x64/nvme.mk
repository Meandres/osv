# global parameter to control the fault injection in the nvme driver
export conf_nvme_fail?=0
export conf_nvme_fail_type?=1
# potential failure types:
# 1.    - succeed IO operation but modify it
# 2.	- fail probabilistically on any operation
# 3.	- fail on any IO operation to a specific block
# 		- add timing to IO operations:
# 4.		+ uniform on all accesses
# 5.		+ uniform with phases on all accesses (condition: temp,... ?)
# 6.		+ random (distribution ?) on all accesses

export conf_nvme_fail_bitflip?=0 # 0/1
export conf_nvme_fail_nth?=0 # 0: deactivated, 1-inf: n
export conf_nvme_fail_block?=0 # 0: deactivated, 1-inf: block id, no check/fail on out-of-bounds
export conf_nvme_fail_timing?=0 # 0: deactivated, 1-inf: time in µs
export conf_nvme_fail_timing_rnd_uplim?=1 # bounds for the rng will be 1-this value
