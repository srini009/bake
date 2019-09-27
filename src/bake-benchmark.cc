#include <iostream>
#include <iomanip>
#include <cmath>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <map>
#include <functional>
#include <memory>
#include <mpi.h>
#include <json/json.h>
#include <bake-client.hpp>
#include <bake-server.hpp>

/**
 * Helper function to generate random strings of a certain length.
 * These strings are readable.
 */
static std::string gen_random_string(size_t len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string s(len, ' ');
    for (unsigned i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return s;
}

template<typename T>
class BenchmarkRegistration;

/**
 * The AbstractBenchmark class describes an interface that a benchmark object
 * needs to satisfy. This interface has a setup, execute, and teardown
 * methods. AbstractBenchmark also acts as a factory to create concrete instances.
 */
class AbstractBenchmark {

    MPI_Comm              m_comm;    // communicator gathering all clients
    bake::client&         m_client;  // bake client
    bake::provider_handle m_bake_ph; // provider handle
    bake::target          m_target;  // bake target

    template<typename T>
    friend class BenchmarkRegistration;

    using benchmark_factory_function = std::function<
        std::unique_ptr<AbstractBenchmark>(Json::Value&, MPI_Comm, bake::client&, const bake::provider_handle&, const bake::target&)>;
    static std::map<std::string, benchmark_factory_function> s_benchmark_factories;

    protected:

    bake::client& client() { return m_client; }
    bake::provider_handle& ph() { return m_bake_ph; }
    bake::target& target() { return m_target; }
    MPI_Comm comm() const { return m_comm; }

    public:

    AbstractBenchmark(MPI_Comm c, bake::client& client, const bake::provider_handle& ph, const bake::target& tgt)
    : m_comm(c)
    , m_client(client)
    , m_bake_ph(ph)
    , m_target(tgt) {}

    virtual ~AbstractBenchmark() = default;
    virtual void setup()    = 0;
    virtual void execute()  = 0;
    virtual void teardown() = 0;

    /**
     * @brief Factory function used to create benchmark instances.
     */
    template<typename ... T>
    static std::unique_ptr<AbstractBenchmark> create(const std::string& type, T&& ... args) {
        auto it = s_benchmark_factories.find(type);
        if(it == s_benchmark_factories.end())
            throw std::invalid_argument(type+" benchmark type unknown");
        return (it->second)(std::forward<T>(args)...);
    }
};

/**
 * @brief The mechanism bellow is used to provide the REGISTER_BENCHMARK macro,
 * which registers a child class of AbstractBenchmark and allows AbstractBenchmark::create("type", ...)
 * to return an instance of this concrete child class.
 */
template<typename T>
class BenchmarkRegistration {
    public:
    BenchmarkRegistration(const std::string& type) {
        AbstractBenchmark::s_benchmark_factories[type] = 
            [](Json::Value& config, MPI_Comm comm, bake::client& clt, const bake::provider_handle& ph, const bake::target& tgt) {
                return std::make_unique<T>(config, comm, clt, ph, tgt);
        };
    }
};

std::map<std::string, AbstractBenchmark::benchmark_factory_function> AbstractBenchmark::s_benchmark_factories;

#define REGISTER_BENCHMARK(__name, __class) \
    static BenchmarkRegistration<__class> __class##_registration(__name)

class AbstractAccessBenchmark : public AbstractBenchmark {
    
    protected:

    uint64_t                  m_num_entries = 0;
    std::pair<size_t, size_t> m_region_size_range;
    bool                      m_erase_on_teardown;

    public:

    template<typename ... T>
    AbstractAccessBenchmark(Json::Value& config, T&& ... args)
    : AbstractBenchmark(std::forward<T>(args)...) {
        // read the configuration
        m_num_entries = config["num-entries"].asUInt64();
        if(config["region-sizes"].isIntegral()) {
            auto x = config["region-sizes"].asUInt64();
            m_region_size_range = { x, x+1 };
        } else if(config["region-sizes"].isArray() && config["region-sizes"].size() == 2) {
            auto x = config["region-sizes"][0].asUInt64();
            auto y = config["region-sizes"][1].asUInt64();
            if(x > y) throw std::range_error("invalid key-sizes range");
            m_region_size_range = { x, y };
        } else {
            throw std::range_error("invalid region-sizes range or value");
        }
        m_erase_on_teardown = config["erase-on-teardown"].asBool();
    }
};

/**
 * CreateBenchmark executes a series of bake_create operations and measures their duration.
 */
class CreateBenchmark : public AbstractAccessBenchmark {

    protected:

    std::vector<size_t>       m_region_sizes;
    std::vector<bake::region> m_region_ids;

    public:

    template<typename ... T>
    CreateBenchmark(T&& ... args)
    : AbstractAccessBenchmark(std::forward<T>(args)...) { }

    virtual void setup() override {
        // generate key/value pairs and store them in the local
        m_region_ids.resize(m_num_entries);
        m_region_sizes.resize(m_num_entries);
        for(unsigned i=0; i < m_num_entries; i++) {
            size_t size = m_region_size_range.first + (rand() % (m_region_size_range.second - m_region_size_range.first));
            m_region_sizes[i] = size;
        }
    }

    virtual void execute() override {
        // execute create operations
        auto& _clt = client();
        auto& _tgt = target();
        auto& _ph = ph();
        for(unsigned i=0; i < m_num_entries; i++) {
            m_region_ids[i] = _clt.create(_ph, _tgt, m_region_sizes[i]);
        }
    }

    virtual void teardown() override {
        if(m_erase_on_teardown) {
            auto& _clt = client();
            auto& _tgt = target();
            auto& _ph = ph();
            for(unsigned i=0; i < m_num_entries; i++) {
                _clt.remove(_ph, m_region_ids[i]);
            }
        }
        m_region_sizes.resize(0); m_region_sizes.shrink_to_fit();
        m_region_ids.resize(0);   m_region_ids.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("create", CreateBenchmark);

/**
 * CreateWritePersistBenchmark executes a series of bake_create_write_persist
 * operations and measures their duration.
 */
class CreateWritePersistBenchmark : public AbstractAccessBenchmark {

    protected:

    std::vector<size_t>       m_region_sizes;
    std::vector<bake::region> m_region_ids;
    std::vector<char>         m_data;
    bool                      m_reuse_buffer;

    public:

    template<typename ... T>
    CreateWritePersistBenchmark(Json::Value& config, T&& ... args)
    : AbstractAccessBenchmark(config, std::forward<T>(args)...) { 
        m_reuse_buffer = config["reuse-buffer"].asBool();
    }

    virtual void setup() override {
        // generate region sizes
        size_t data_size = 0;
        m_region_ids.resize(m_num_entries);
        m_region_sizes.resize(m_num_entries);
        for(unsigned i=0; i < m_num_entries; i++) {
            size_t size = m_region_size_range.first + (rand() % (m_region_size_range.second - m_region_size_range.first));
            m_region_sizes[i] = size;
            if(m_reuse_buffer) data_size = std::max(size, data_size);
            else data_size += size;
        }
        m_data.resize(data_size);
        for(unsigned i=0; i < data_size; i++) {
            m_data[i] = 'a' + (i%26);
        }
    }

    virtual void execute() override {
        auto& _clt = client();
        auto& _tgt = target();
        auto& _ph = ph();
        size_t offset = 0;
        for(unsigned i=0; i < m_num_entries; i++) {
            size_t size = m_region_sizes[i];
            char* data = m_reuse_buffer ? m_data.data() : (m_data.data() + offset);
            offset += size;
            m_region_ids[i] = _clt.create_write_persist(_ph, _tgt, (void*)data, m_region_sizes[i]);
        }
    }

    virtual void teardown() override {
        if(m_erase_on_teardown) {
            auto& _clt = client();
            auto& _tgt = target();
            auto& _ph = ph();
            for(unsigned i=0; i < m_num_entries; i++) {
                _clt.remove(_ph, m_region_ids[i]);
            }
        }
        m_region_sizes.resize(0); m_region_sizes.shrink_to_fit();
        m_region_ids.resize(0);   m_region_ids.shrink_to_fit();
        m_data.resize(0);         m_data.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("create-write-persist", CreateWritePersistBenchmark);

/**
 * ReadBenchmark executes a series of bake_create_write_persist
 * the a series of bake_read operations and measures the duration of the latter.
 */
class ReadBenchmark : public AbstractAccessBenchmark {

    protected:

    std::vector<size_t>       m_region_sizes;
    std::vector<bake::region> m_region_ids;
    std::vector<char>         m_write_data;
    std::vector<char>         m_read_data;
    bool                      m_reuse_buffer;

    public:

    template<typename ... T>
    ReadBenchmark(Json::Value& config, T&& ... args)
    : AbstractAccessBenchmark(config, std::forward<T>(args)...) { 
        m_reuse_buffer = config["reuse-buffer"].asBool();
    }

    virtual void setup() override {
        auto& _clt = client();
        auto& _tgt = target();
        auto& _ph = ph();
        // generate region sizes
        size_t write_data_size = 0;
        size_t read_data_size = 0;
        m_region_ids.reserve(m_num_entries);
        m_region_sizes.reserve(m_num_entries);
        for(unsigned i=0; i < m_num_entries; i++) {
            size_t size = m_region_size_range.first + (rand() % (m_region_size_range.second - m_region_size_range.first));
            m_region_sizes[i] = size;
            write_data_size = std::max(size, write_data_size);
            if(m_reuse_buffer) read_data_size = std::max(size, read_data_size);
            else read_data_size += size;
        }
        m_write_data.resize(write_data_size);
        for(unsigned i=0; i < write_data_size; i++) {
            m_write_data[i] = 'a' + (i%26);
        }
        for(unsigned i=0; i < m_num_entries; i++) {
            m_region_ids[i] = _clt.create_write_persist(_ph, _tgt, (void*)m_write_data.data(), m_region_sizes[i]);
        }
        m_read_data.resize(read_data_size);
    }

    virtual void execute() override {
        auto& _clt = client();
        auto& _tgt = target();
        auto& _ph = ph();
        size_t offset = 0;
        for(unsigned i=0; i < m_num_entries; i++) {
            size_t size = m_region_sizes[i];
            char* data = m_reuse_buffer ? m_read_data.data() : (m_read_data.data() + offset);
            offset += size;
            _clt.read(_ph, m_region_ids[i], 0, (void*)data, m_region_sizes[i]);
        }
    }

    virtual void teardown() override {
        if(m_erase_on_teardown) {
            auto& _clt = client();
            auto& _tgt = target();
            auto& _ph = ph();
            for(unsigned i=0; i < m_num_entries; i++) {
                _clt.remove(_ph, m_region_ids[i]);
            }
        }
        m_region_sizes.resize(0); m_region_sizes.shrink_to_fit();
        m_region_ids.resize(0);   m_region_ids.shrink_to_fit();
        m_read_data.resize(0);    m_read_data.shrink_to_fit();
        m_write_data.resize(0);   m_write_data.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("read", ReadBenchmark);

static void run_server(MPI_Comm comm, Json::Value& config);
static void run_client(MPI_Comm comm, Json::Value& config);

/**
 * @brief Main function.
 */
int main(int argc, char** argv) {

    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if(argc != 2) {
        if(rank == 0) {
            std::cerr << "Usage: " << argv[0] << " <config.json>" << std::endl;
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
    }

    std::ifstream config_file(argv[1]);
    if(!config_file.good() && rank == 0) {
        std::cerr << "Could not read configuration file " << argv[1] << std::endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    Json::Reader reader;
    Json::Value config;
    reader.parse(config_file, config);

    MPI_Comm comm;
    MPI_Comm_split(MPI_COMM_WORLD, rank == 0 ? 0 : 1, rank, &comm);

    if(rank == 0) {
        run_server(comm, config);
    } else {
        run_client(comm, config);
    }

    MPI_Finalize();
    return 0;
}

static void run_server(MPI_Comm comm, Json::Value& config) {
    // initialize Margo
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    std::string protocol = config["protocol"].asString();
    auto& server_config = config["server"];
    bool use_progress_thread = server_config["use-progress-thread"].asBool();
    int  rpc_thread_count = server_config["rpc-thread-count"].asInt();
    mid = margo_init(protocol.c_str(), MARGO_SERVER_MODE, use_progress_thread, rpc_thread_count);
    margo_enable_remote_shutdown(mid);
    // serialize server address
    std::vector<char> server_addr_str(256,0);
    hg_size_t buf_size = 256;
    hg_addr_t server_addr = HG_ADDR_NULL;
    margo_addr_self(mid, &server_addr);
    margo_addr_to_string(mid, server_addr_str.data(), &buf_size, server_addr);
    margo_addr_free(mid, server_addr);
    // send server address to client
    MPI_Bcast(&buf_size, sizeof(hg_size_t), MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(server_addr_str.data(), buf_size, MPI_BYTE, 0, MPI_COMM_WORLD);
    // initialize sdskv provider
    auto provider = bake::provider::create(mid);
    // initialize database
    auto& target_config = server_config["target"];
    std::string tgt_path = target_config["path"].asString();
    provider->add_storage_target(tgt_path);
    // notify clients that the database is ready
    MPI_Barrier(MPI_COMM_WORLD);
    // wait for finalize
    margo_wait_for_finalize(mid);
}

static void run_client(MPI_Comm comm, Json::Value& config) {
    // get info from communicator
    int rank, num_clients;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &num_clients);
    Json::StyledStreamWriter styledStream;
    // initialize Margo
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    std::string protocol = config["protocol"].asString();
    mid = margo_init(protocol.c_str(), MARGO_SERVER_MODE, 0, 0);
    // receive server address
    std::vector<char> server_addr_str;
    hg_size_t buf_size;
    hg_addr_t server_addr = HG_ADDR_NULL;
    MPI_Bcast(&buf_size, sizeof(hg_size_t), MPI_BYTE, 0, MPI_COMM_WORLD);
    server_addr_str.resize(buf_size, 0);
    MPI_Bcast(server_addr_str.data(), buf_size, MPI_BYTE, 0, MPI_COMM_WORLD);
    margo_addr_lookup(mid, server_addr_str.data(), &server_addr);
    // wait for server to have initialize the database
    MPI_Barrier(MPI_COMM_WORLD);
    {
        // open remote database
        bake::client client(mid);
        bake::provider_handle ph(client, server_addr);
        std::vector<bake::target> targets = client.probe(ph);
        bake::target target = targets[0];
        // initialize the RNG seed
        int seed = config["seed"].asInt();
        // initialize benchmark instances
        std::vector<std::unique_ptr<AbstractBenchmark>> benchmarks;
        std::vector<unsigned> repetitions;
        std::vector<std::string> types;
        benchmarks.reserve(config["benchmarks"].size());
        repetitions.reserve(config["benchmarks"].size());
        types.reserve(config["benchmarks"].size());
        for(auto& bench_config : config["benchmarks"]) {
            std::string type = bench_config["type"].asString();
            types.push_back(type);
            benchmarks.push_back(AbstractBenchmark::create(type, bench_config, comm, client, ph, target));
            repetitions.push_back(bench_config["repetitions"].asUInt());
        }
        // main execution loop
        for(unsigned i = 0; i < benchmarks.size(); i++) {
            auto& bench  = benchmarks[i];
            unsigned rep = repetitions[i];
            // reset the RNG
            srand(seed + rank*1789);
            std::vector<double> local_timings(rep);
            for(unsigned j = 0; j < rep; j++) {
                MPI_Barrier(comm);
                // benchmark setup
                bench->setup();
                MPI_Barrier(comm);
                // benchmark execution
                double t_start = MPI_Wtime();
                bench->execute();
                double t_end = MPI_Wtime();
                local_timings[j] = t_end - t_start;
                MPI_Barrier(comm);
                // teardown
                bench->teardown();
            }
            // exchange timings
            std::vector<double> global_timings(rep*num_clients);
            if(num_clients != 1) {
                MPI_Gather(local_timings.data(), local_timings.size(), MPI_DOUBLE,
                       global_timings.data(), local_timings.size(), MPI_DOUBLE, 0, comm);
            } else {
                std::copy(local_timings.begin(), local_timings.end(), global_timings.begin());
            }
            // print report
            if(rank == 0) {
                size_t n = global_timings.size();
                std::cout << "================ " << types[i] << " ================" << std::endl;
                styledStream.write(std::cout, config["benchmarks"][i]);
                std::cout << "-----------------" << std::string(types[i].size(),'-') << "-----------------" << std::endl;
                double average  = std::accumulate(global_timings.begin(), global_timings.end(), 0.0) / n;
                double variance = std::accumulate(global_timings.begin(), global_timings.end(), 0.0, [average](double acc, double x) {
                        return acc + std::pow((x - average),2);
                    });
                variance /= n;
                double stddev = std::sqrt(variance);
                std::sort(global_timings.begin(), global_timings.end());
                double min = global_timings[0];
                double max = global_timings[global_timings.size()-1];
                double median = (n % 2) ? global_timings[n/2] : ((global_timings[n/2] + global_timings[n/2 + 1])/2.0);
                double q1 = global_timings[n/4];
                double q3 = global_timings[(3*n)/4];
                std::cout << std::setprecision(9) << std::fixed;
                std::cout << "Samples         : " << n << std::endl;
                std::cout << "Average(sec)    : " << average << std::endl;
                std::cout << "Variance(sec^2) : " << variance << std::endl;
                std::cout << "StdDev(sec)     : " << stddev << std::endl;
                std::cout << "Minimum(sec)    : " << min << std::endl;
                std::cout << "Q1(sec)         : " << q1 << std::endl;
                std::cout << "Median(sec)     : " << median << std::endl;
                std::cout << "Q3(sec)         : " << q3 << std::endl;
                std::cout << "Maximum(sec)    : " << max << std::endl;
            }
        }
        // wait for all the clients to be done with their tasks
        MPI_Barrier(comm);
        // shutdown server and finalize margo
        if(rank == 0)
            margo_shutdown_remote_instance(mid, server_addr);
    }
    margo_addr_free(mid, server_addr);
    margo_finalize(mid);
}
