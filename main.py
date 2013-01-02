from multiprocessing import Process, Pipe, Queue
from copy import deepcopy
from datetime import datetime

class Node(object):
    def __init__(self):
        self.leftSon = None
        self.rightSon = None

    def reset(self):
        pass

    def next(self):
        pass

    def create_copy(self, process_num, process_count, connections, port_count):
        pass

class RootNode(Node):
    def __init__(self):
        super(RootNode, self).__init__()
        self.leftSon = None
        self.rightSon = None

    def reset(self):
        self.leftSon.reset()

    def next(self):
        return self.leftSon.next()

    def run(self):
        self.reset()
        result = []
        out_tuple = self.next()
        while out_tuple:
            if not out_tuple in result:
                result.append(out_tuple)
            out_tuple = self.next()
        if hasattr(self, 'process_num'):
            if self.process_num == 0:
                self.result_queue.put(result)
        return result

    def parallel_run(self, process_count):
        topExchange = TopExchangeNode(port = 0)
        topExchange.leftSon = self.leftSon
        self.leftSon = topExchange
        self.reset()
        conn = []

        for p in range(3):
            conn.append([])
            for i in range(process_count):
                conn[p].append([])
                for j in range(process_count):
                    conn[p][i].append(None)
                    if i>j:
                        conn[p][i][j] = Queue()
                        conn[p][j][i] = Queue()

        conn[0][0][0] = Queue()
        root_nodes = []
        for i in range(process_count):
            root_nodes.append(self.create_copy(i, process_count, conn, 1))

        p = []
        for i in range(len(root_nodes)):
            p.append(Process(target = root_nodes[i].run, args = () ))
            p[i].start()

        for process in p:
            process.join()
        result = conn[0][0][0].get()
        return result

    def create_copy(self, process_num, process_count, connections, port_count):
        result = deepcopy(self)
        result.process_num = process_num
        result.result_queue = connections[0][0][0]
        if not result.leftSon is None:
            result.leftSon = self.leftSon.create_copy(process_num, process_count, connections, port_count)
        return result


class RelationNode(Node):
    def __init__(self, filename = None) :
        super(RelationNode, self).__init__()
        if filename:
            self.load_from_file(filename)

    def load_from_file(self, filename):
        file = open(filename)
        self.name = file.readline().replace("\n", "")
        self.heading = file.readline().split()
        for i in range(len(self.heading)):
            self.heading[i] = self.name + "." + self.heading[i]
        self.tuples = []
        lines = file.readlines()
        for line in lines:
            self.tuples.append(dict(zip(self.heading, [int(value) for value in line.split()])))
        self.fragmentation_field = self.heading[0]

    def reset(self):
        self._iter = 0

    def next(self):
        if len(self.tuples) == self._iter:
            result = None
        else:
            result = self.tuples[self._iter]
        self._iter += 1
        return result

    def fragmentation_func(self, tuple, process_count):
        return tuple[self.fragmentation_field] / process_count % process_count

    def create_copy(self, process_num, process_count, connections, port_count):
        result = deepcopy(self)
        result.name = self.name + "-" + str(process_num)
        result.tuples = []
        for tuple in self.tuples:
            if result.fragmentation_func(tuple, process_count) == process_num:
                result.tuples.append(tuple)
        return result


class ExchangeNode(Node):
    def __init__(self, port, process_count = None, process_num = None, connections = None):
        super(ExchangeNode, self).__init__()
#        print process_num
        self.port = port
        self.process_count = process_count
        self.process_num = process_num
        self.connections = connections

    def create_copy(self, process_num, process_count, connections, port_count):
        result = deepcopy(self)
        result.process_num = process_num
        result.process_count = process_count
        result.connections = connections
        result.leftSon = self.leftSon.create_copy(process_num, process_count, connections, port_count)
        return result

    def send_func(self, tuple = None):
        pass

    def reset(self):
        self.gather_none_count = 0
        self.my_tuples_left = True
        self.leftSon.reset()

    def is_all_gathered(self):
        return self.gather_none_count == self.process_count - 1

    def next(self):
        if not self.is_all_gathered():
            gathered_tuple, is_result = self.gather_next()
            if is_result and gathered_tuple is not None:
                return gathered_tuple
        if self.my_tuples_left:
            tuple = self.leftSon.next()
            if tuple is None:
                self.scatter_all(None)
                self.my_tuples_left = False
                if self.is_all_gathered():
                    return None # finish
                else:
                    return self.next()
            if self.send_func(tuple) == self.process_num:
                return tuple
            else:
                self.scatter_next(tuple)
                return self.next()
        else:
            if self.is_all_gathered():
                return None
            else:
                return self.next()

    def scatter_next(self, tuple):
        dest = self.send_func(tuple)
#        print "sent from", self.process_num, "to", dest, "connection", tuple
        self.connections[self.port][self.process_num][dest].put(tuple)

    def scatter_all(self, tuple):
        for dest in range(self.process_count):
            if self.connections[self.process_num][dest] is not None and dest != self.process_num:
#                print "sent from", self.process_num, "to", dest, tuple
                self.connections[self.port][self.process_num][dest].put(tuple)

    def gather_next(self):
        for i in range(self.process_count):
            if self.connections[self.port][i][self.process_num] is not None and not self.connections[self.port][i][self.process_num].empty():
                tuple = self.connections[self.port][i][self.process_num].get()
#                print "gather in", self.process_num, "from", i, tuple
                if tuple is not None:
                    return tuple, True
                else:
                    self.gather_none_count += 1
                    if self.is_all_gathered():
                        return tuple, True
        return None, False


class TopExchangeNode(ExchangeNode):
    def __init__(self, port, process_count = None, process_num = None, connections = None):
        super(TopExchangeNode, self).__init__(port, process_count, process_num, connections)

    def send_func(self, tuple = None, process_count = None):
        return 0

class CartExchangeNode(ExchangeNode):
    def __init__(self, port, process_count = None, process_num = None, connections = None):
        super(CartExchangeNode, self).__init__(port, process_count, process_num, connections)

    def send_func(self, tuple = None, process_count = None):
        return range(self.process_count)

    def next(self):
        if not self.is_all_gathered():
                gathered_tuple, is_result = self.gather_next()
                if is_result and gathered_tuple is not None:
                    return gathered_tuple
        if self.my_tuples_left:
            tuple = self.leftSon.next()
            if tuple is None:
                self.scatter_all(None)
                self.my_tuples_left = False
                if self.is_all_gathered():
                    return None # finish
                else:
                    return self.next()
            self.scatter_all(tuple)
            return tuple
        else:
            if self.is_all_gathered():
                return None
            else:
                return self.next()


class SelectionNode(Node):
    def __init__(self):
        super(SelectionNode, self).__init__()
        self.conditions = []
        self.from_list = []
        self.is_tree_built = False

    def _build_tree(self):
        if len(self.from_list) == 1:
            self.leftSon = self.from_list[0]
        else:
            relations_left = len(self.from_list)
            cart_product = CartProductNode()
            self.leftSon = cart_product
            while relations_left != 0:
                relations_left -= 1
                cart_product.leftSon = self.from_list[relations_left]
                if relations_left == 1:
                    relations_left -= 1
                    cart_product.rightSon = self.from_list[relations_left]
                else:
                    new_cart_product = CartProductNode()
                    cart_product.rightSon = new_cart_product
                    cart_product = new_cart_product
        self.is_tree_built = True

    def reset(self):
        if not self.is_tree_built:
            self._build_tree()
        self.leftSon.reset()
        self._in_condition_relation = None

    def _tuple_meets_conditions(self, tuple):
        is_next = True
        for condition in self.conditions:
            if not self._tuple_meets_condition(tuple, condition):
                is_next = False
        return is_next

    def _tuple_meets_condition(self, tuple, condition):
        if condition["operation"] == "in":
            return self._tuple_meets_in_condition(tuple, condition)

        if type(condition["value"]) == type(1):
            value = condition["value"]
        else:
            value = tuple[condition["value"]]

        if condition["operation"] == "=":
            return tuple[condition["argument"]] == value
        if condition["operation"] == "!=":
            return tuple[condition["argument"]] != value
        if condition["operation"] == "<":
            return tuple[condition["argument"]] < value
        if condition["operation"] == ">":
            return tuple[condition["argument"]] > value
        if condition["operation"] == ">=":
            return tuple[condition["argument"]] >= value
        if condition["operation"] == "<=":
            return tuple[condition["argument"]] <= value
        if condition["operation"] == "<>":
            return tuple[condition["argument"]] <> value

    def _tuple_meets_in_condition(self, tuple, condition):
        if not self._in_condition_relation:
            self._in_condition_relation = condition["value"].run()

        for tuple_inside in self._in_condition_relation:
            for key, value in tuple_inside.iteritems():
                if tuple[condition["argument"]] == value:
                    return True

        return False

    def next(self):
        while True:
            tuple = self.leftSon.next()
            if tuple is None:
                return None
            if self._tuple_meets_conditions(tuple):
                return tuple

    def create_copy(self, process_num, process_count, connections, port_count):
        result = deepcopy(self)
        result.leftSon = self.leftSon.create_copy(process_num, process_count, connections, port_count)
        return result

class CartProductNode(Node):
    def __init__(self):
        super(CartProductNode, self).__init__()

    def reset(self):
        self.leftSon.reset()
        self.rightSon.reset()
        self.left_tuple = self.leftSon.next()
        self.right_tuple = self.rightSon.next()

    def next(self):
        if self.right_tuple is None:
            self.left_tuple = self.leftSon.next()
            if self.left_tuple is None:
                return None
            self.rightSon.reset()
            self.right_tuple = self.rightSon.next()
        result = dict(self.left_tuple.items() + self.right_tuple.items())
        self.right_tuple = self.rightSon.next()
        return result

    def create_copy(self, process_num, process_count, connections, port_count):
        result = deepcopy(self)
        result.leftSon = CartExchangeNode(port_count, process_count=process_count, process_num=process_num, connections=connections)
        result.leftSon.leftSon = self.leftSon.create_copy(process_num, process_count, connections, port_count+2)
        result.rightSon = CartExchangeNode(port_count+1, process_count=process_count, process_num=process_num, connections=connections)
        result.rightSon.leftSon = self.rightSon.create_copy(process_num, process_count, connections, port_count+2)
        return result

class ProjectionNode(Node):
    def __init__(self):
        super(ProjectionNode, self).__init__()
        self.attr_list = []

    def reset(self):
        self.leftSon.reset()

    def next(self):
        tuple = self.leftSon.next()
        if tuple is None:
            return None
        else:
            return dict((attr, tuple[attr]) for attr in self.attr_list)

    def create_copy(self, process_num, process_count, connections, port_count):
        result = deepcopy(self)
        result.leftSon = self.leftSon.create_copy(process_num, process_count, connections, port_count)
        return result

if __name__ == '__main__':
    pass