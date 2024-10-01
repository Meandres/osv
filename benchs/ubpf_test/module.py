from osv.modules import api

api.require('ubpf')
default = api.run(cmdline="/ubpf_test")
