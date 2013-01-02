from main import *

def parse_query(str, relations):
    root = RootNode()
    selections = str.split("\n")
    args = selections[0].split(";")
    nested_selections = "\n".join(selections[1:])
    root.leftSon = _parse_select(args, relations, nested_selections)
    return root

def _parse_relations(relations_str, relations):
    result = []
    relations_names = relations_str.split(",")
    for relation_name in relations_names:
        for relation in relations:
            if relation.name == relation_name:
                result.append(relation)
    return result

def _parse_conditions(conditions_str, relations, nested_selections):
    result = []
    for condition_str in conditions_str.split(","):
        condition_parts = condition_str.split(" ")
        try:
            value = condition_parts[2]
            try:
                value = int(value)
            except:
                pass
            result.append({"argument":condition_parts[0], "operation":condition_parts[1], "value":value})
        except:
            result.append({"argument":condition_parts[0], "operation":"in", "value":parse_query(nested_selections, relations)})
    return result

def _parse_select(args, relations, nested_selections):
    selection = SelectionNode()
    selection.from_list = _parse_relations(args[1],relations)
    selection.conditions = _parse_conditions(args[2], relations, nested_selections)
    if args[0] <> "*":
        projection = ProjectionNode()
        projection.attr_list = args[0].split(",")
        projection.leftSon = selection
        return projection
    else:
        return selection



