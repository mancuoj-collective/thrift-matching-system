#include "Match.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include <iostream>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace ::match_service;

using namespace std;

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>

struct Task {
    User user;
    string type;
};

struct MsgQueue {
    queue<Task> q;
    mutex m;
    condition_variable cv;
}msg_queue;

class Pool {
    public:
        void save_result(int a, int b) {
            printf("Match Result: %d %d\n", a, b);
        }

        void match() {
            while (users.size() > 1) {
                auto a = users[0], b = users[1];
                users.erase(users.begin());
                users.erase(users.begin());
                save_result(a.id, b.id);
            }
        }

        void add(User user) {
            users.push_back(user);
        }

        void remove(User user) {
            for (uint32_t i = 0; i < users.size(); i++){
                if (users[i].id == user.id) {
                    users.erase(users.begin()+i);
                    break;
                }
            }
        }
    private:
        vector<User> users;
}pool;

class MatchHandler : virtual public MatchIf {
    public:
        MatchHandler() {
        }

        int32_t add_user(const User& user, const std::string& info) {
            printf("add_user\n");

            unique_lock<mutex> lck(msg_queue.m);
            msg_queue.q.push({user, "add"});
            msg_queue.cv.notify_all();

            return 0;
        }

        int32_t remove_user(const User& user, const std::string& info) {
            printf("remove_user\n");

            unique_lock<mutex> lck(msg_queue.m);
            msg_queue.q.push({user, "remove"});
            msg_queue.cv.notify_all();

            return 0;
        }
};


void consume_task() {
    while(true) {
        unique_lock<mutex> lck(msg_queue.m);
        if (msg_queue.q.empty()) {
            msg_queue.cv.wait(lck);
        } else {
            auto task = msg_queue.q.front();
            msg_queue.q.pop();
            lck.unlock();
            
            if (task.type == "add") pool.add(task.user);
            else if (task.type == "remove") pool.remove(task.user);

            pool.match();
        }
    }
}

int main(int argc, char **argv) {
    int port = 9090;
    ::std::shared_ptr<MatchHandler> handler(new MatchHandler());
    ::std::shared_ptr<TProcessor> processor(new MatchProcessor(handler));
    ::std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    ::std::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    ::std::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);

    cout << "Start match server 😎" << endl;
    thread matching_thread(consume_task);

    server.serve();
    return 0;
}

