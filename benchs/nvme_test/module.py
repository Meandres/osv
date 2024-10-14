from osv.modules import api

api.require('fio')
ioengine = api.run('/fio --ioengine=/ioengine_unvme /benchmarks.fio')
