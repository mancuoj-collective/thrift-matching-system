from match import Match
from match.ttypes import User

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from sys import stdin


def operate(op, user_id, username, score):
    transport = TSocket.TSocket('localhost', 9090)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Match.Client(protocol)
    transport.open()

    user = User(user_id, username, score)
    if op == "add":
        client.add_user(user, "")
    elif op == "remove":
        client.remove_user(user, "")

    transport.close()


def main():
    for line in stdin:
        op, user_id, username, score = line.split(" ")
        operate(op, int(user_id), username, int(score))


if __name__ == "__main__":
    main()
