from osv.modules import api

api.require('fio')
#default = api.run("/nvme_test")
ioengine = api.run('/fio --ioengine=/ioengine_nvme /benchmarks.fio')
