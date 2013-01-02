import unittest
from main import *
from parser import parse_query
from multiprocessing import Process

def compare(s, t):
    t = list(t)   # make a mutable copy
    try:
        for elem in s:
            t.remove(elem)
    except ValueError:
        return False
    return not t


class PDBMSTestCase(unittest.TestCase):
    def test_relation_input(self):
        relation = RelationNode()
        relation.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        self.assertEqual(relation.name,"R")
        self.assertEqual(relation.heading, ["R.A", "R.B", "R.C"])
        self.assertEqual(len(relation.tuples), 4)
        self.assertEqual(relation.tuples[1], {"R.A":4, "R.B":5, "R.C":6})

    def test_equal_selection(self):
        relation = RelationNode()
        relation.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        selection = SelectionNode()
        selection.from_list = [relation, ]
        root = RootNode()
        root.leftSon = selection
        selection.conditions = [{"argument":"R.A", "operation":"=", "value":1}, ]
        result = root.run()
        self.assertEqual(len(result), 2)

    def test_less_selection(self):
        relation = RelationNode()
        relation.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        selection = SelectionNode()
        selection.from_list = [relation, ]
        root = RootNode()
        root.leftSon = selection
        selection.conditions = [{"argument":"R.B", "operation":"<", "value":6}, ]
        result = root.run()
        self.assertEqual(len(result), 2)

    def test_more_selection(self):
        relation = RelationNode()
        relation.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        selection = SelectionNode()
        selection.from_list = [relation, ]
        root = RootNode()
        root.leftSon = selection
        selection.conditions = [{"argument":"R.C", "operation":">", "value":3}, ]
        result = root.run()
        self.assertEqual(len(result), 3)

    def test_cart_product(self):
        relationR = RelationNode()
        relationR.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        relationS = RelationNode()
        relationS.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/S.txt")
        selection = SelectionNode()
        selection.from_list = [relationR, relationS]
        root = RootNode()
        root.leftSon = selection
        selection.conditions = [{"argument":"R.A", "operation":"=", "value":"S.A"}, ]
        result = root.run()
        self.assertEqual(len(result), 2)

    def test_multi_level_select(self):
        relationR = RelationNode()
        relationR.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        relationS = RelationNode()
        relationS.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/S.txt")
        selection = SelectionNode()
        selection.from_list = [relationR, ]
        selectionInside = SelectionNode()
        selectionInside.from_list = [relationS, ]
        selectionInside.conditions = [{"argument":"S.D", "operation":"<", "value":5}, ]
        root = RootNode()
        root.leftSon = selection
        rootInside = RootNode()
        rootInside.leftSon = selectionInside
        selection.conditions = [{"argument":"R.B", "operation":"in", "value":rootInside}, ]
        result = root.run()
        self.assertEqual(len(result), 1)

    def test_multi_level_select_and_condition(self):
        relationR = RelationNode()
        relationR.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        relationS = RelationNode()
        relationS.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/S.txt")
        selection = SelectionNode()
        selection.from_list = [relationR, ]
        selectionInside = SelectionNode()
        selectionInside.from_list = [relationS, ]
        selectionInside.conditions = [{"argument":"S.D", "operation":"<", "value":5}, ]
        root = RootNode()
        root.leftSon = selection
        rootInside = RootNode()
        rootInside.leftSon = selectionInside
        selection.conditions = [{"argument":"R.B", "operation":"in", "value":rootInside}, {"argument":"R.B", "operation":"<>", "value":2}]
        result = root.run()
        self.assertEqual(len(result), 0)

    def test_projection_selection(self):
        relation = RelationNode()
        relation.load_from_file("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        selection = SelectionNode()
        selection.from_list = [relation, ]
        selection.conditions = [{"argument":"R.A", "operation":">=", "value":7}, ]
        projection = ProjectionNode()
        projection.attr_list = ["R.B", "R.C"]
        projection.leftSon = selection
        root = RootNode()
        root.leftSon = projection
        result = root.run()
        self.assertEqual(result[0], {"R.B":8, "R.C":9})

    def test_parser_simple_query(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        query = "R.B;R;R.A = 1"
        root = parse_query(query, [relationR, ])
        result = root.run()
        self.assertEqual(result, [{"R.B":2}, {"R.B":10}])

    def test_parser_simple_query_all_attr(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        query = "*;R;R.A = 1"
        root = parse_query(query, [relationR, ])
        result = root.run()
        self.assertEqual(result, [{"R.A":1, "R.B":2, "R.C":3}, {"R.A":1, "R.B":10, "R.C":11}])

    def test_parser_simple_query_two_relations(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        relationS = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/S.txt")
        query = "R.A,R.B,S.D;R,S;R.A = S.A,R.A = 4"
        root = parse_query(query, [relationR, relationS])
        result = root.run()
        self.assertEqual(result, [{"R.A":4, "R.B":5, "S.D":9}])

    def test_parser_simple_query_two_conditions(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        query = "R.B;R;R.A = 1,R.C <> 3"
        root = parse_query(query, [relationR, ])
        result = root.run()
        self.assertEqual(result, [{"R.B":10}, ])

    def test_parser_simple_query(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        query = "R.B;R;R.A = 1"
        root = parse_query(query, [relationR, ])
        result = root.run()
        self.assertEqual(result, [{"R.B":2}, {"R.B":10}])

    def test_parser_nested_query(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        relationS = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/S.txt")
        query = "*;R;R.B in\nS.D;S;S.D < 5"
        root = parse_query(query, [relationR, relationS])
        result = root.run()
        self.assertEqual(result, [{"R.A":1, "R.B":2, "R.C":3}, ])

    def test_parallel_simple_selection(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        query = "*;R;R.B != 5"
        root = parse_query(query, [relationR, ])
        result = root.parallel_run(2)
        self.assertEqual(compare(result, [{"R.A":1, "R.B":2, "R.C":3},
                                  {"R.A":7, "R.B":8, "R.C":9},
                                  {"R.A":1, "R.B":10, "R.C":11}]), True)

    def test_parallel_simple_selection2(self):
        relationS = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/S.txt")
        query = "S.D;S;S.A > 5"
        root = parse_query(query, [relationS, ])
        result = root.parallel_run(3)
        self.assertEqual(compare(result, [{"S.D":3},
                                          {"S.D":2}]), True)

    def test_parallel_two_relations_fragmentation_is_send_func(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        relationS = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/S.txt")
        query = "R.A,R.B,S.D;R,S;R.A = S.A"
        root = parse_query(query, [relationS, relationR])
        result = root.parallel_run(2)
        self.assertEqual(compare(result, [{"R.A":4, "R.B":5, "S.D":9},
                                            {"R.A":7, "R.B":8, "S.D":3}]), True)

    def test_parallel_two_relations_fragmentation_is_not_send_func(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        relationP = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/P.txt")
        query = "R.A,R.B,P.D;R,P;R.A = P.A"
        root = parse_query(query, [relationP, relationR])
        result = root.parallel_run(2)
        self.assertEqual(compare(result, [{"R.A":4, "R.B":5, "P.D":9},
                                          {"R.A":7, "R.B":8, "P.D":3}]), True)

    def test_parallel_two_relations_fragmentation_is_not_send_func2(self):
        relationR = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/R.txt")
        relationP = RelationNode("/Users/Lena/Documents/Study/Parallel_DB/P.txt")
        query = "R.C,P.D;R,P;R.A > 1,P.D = 9"
        root = parse_query(query, [relationP, relationR])
        result = root.parallel_run(2)
        self.assertEqual(compare(result, [{"R.C":6, "P.D":9},
                                          {"R.C":9, "P.D":9}]), True)


if __name__ == '__main__':
    unittest.main()
