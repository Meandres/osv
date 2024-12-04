/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 * This work is open source software, licensed under the terms of the
 * BSD license as described in the LICENSE file in the top-level directory.
 */

#ifndef _OSV_SAMPLER_HH
#define _OSV_SAMPLER_HH

#include <osv/clock.hh>

namespace prof {

struct config {
    osv::clock::uptime::duration period;
};

/**
 * Starts the sampler.
 *
 * If sampler is already running it is stopped and restarted with the new config atomically.
 *
 * May block.
 */
void start_sampler(config) throw();

/**
 * Stops the sampler.
 *
 * May block.
 */
void stop_sampler() throw();

typedef struct thread_runtime_checkpoint {
    uint64_t counter;
    std::string name;
    long int runtime; 
} trc_t;
extern std::vector<trc_t> thread_runtime_log;
void print_log();
}

#endif
