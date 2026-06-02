#ifndef THREAD_HXX_
#define THREAD_HXX_

#if USE_CLICKHOUSE_THREADS
#include <Common/ThreadPool.h>
#else
#include <thread>
#endif

namespace nuraft
{
#if USE_CLICKHOUSE_THREADS
    using nuraft_thread = ThreadFromGlobalPool;
#else
    using nuraft_thread = std::thread;
#endif
}


#endif // THREAD_HXX_
