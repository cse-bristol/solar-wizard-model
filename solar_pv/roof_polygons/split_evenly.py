from typing import Tuple, List

from networkx import Graph, cycle_basis
from shapely import Polygon, LineString, ops, Point, MultiPoint, MultiLineString
from shapely.geometry import CAP_STYLE, JOIN_STYLE

from solar_pv.geos import multi, densify_polygon, geoms, largest_polygon, fill_holes

_VORONOI_DENSITY_MIN = 0.001


def split_evenly(p1: Polygon, p2: Polygon,
                 min_area: float = 0.25,
                 min_dist_between_planes: float = 0.1,
                 voronoi_point_density: float = 0.1,
                 debug: bool = False) -> Tuple[Polygon, Polygon]:
    """
    Split 2 overlapping polygons evenly.

    If the overlap is simple - i.e. a straight line between the points where the
    boundaries of p1 and p2 overlap doesn't go outside the overlap - just split on that
    line.

    Otherwise, use an algorithm built on the one described in this paper
    https://proceedings.esri.com/library/userconf/proc96/TO400/PAP370/P370.HTM for
    finding the centre-lines of roads.
    """
    p1_fh = fill_holes(p1)
    p2_fh = fill_holes(p2)
    if p1_fh.contains(p2) or p1_fh.intersection(p2).area > 0.9 * p2.area:
        return largest_polygon(p1.difference(p2.buffer(min_dist_between_planes))), p2
    if p2_fh.contains(p1) or p2_fh.intersection(p1).area > 0.9 * p1.area:
        return p1, largest_polygon(p2.difference(p1.buffer(min_dist_between_planes)))

    overlap = p1.intersection(p2)
    if overlap is None or overlap.is_empty:
        if debug:
            print(f"planes do not overlap")
        return p1, p2

    overlap = multi(overlap)
    splitter = []

    if debug:
        print(f"planes overlap in {len(overlap.geoms)} parts")

    for overlap_part in overlap.geoms:
        if overlap_part.geom_type != 'Polygon' or overlap_part.area < min_area:
            continue
        overlap_part = fill_holes(overlap_part)

        # Check if this is a simple overlap where the line between the 2 points
        # where the boundaries of p1 and p2 overlap is (nearly completely) contained
        # within the overlap
        straight_central_line = []
        b_intersect = p1.boundary.intersection(p2.boundary)
        for g in b_intersect.geoms:
            if not g.intersects(overlap_part):
                continue
            if g.geom_type == 'Point':
                straight_central_line.append(g)
            elif g.geom_type == 'LineString':
                intersect = g.intersection(overlap_part)
                straight_central_line.append(intersect.centroid if intersect else g.centroid)
            else:
                raise ValueError(f"Intersection of boundary of 2 roof planes was not point or linestring: was {g.geom_type}")

        straight_central_line = LineString(straight_central_line)
        if overlap_part.buffer(0.1).contains(straight_central_line):
            if debug:
                print(f"simple overlap")
            splitter.append(straight_central_line.buffer(min_dist_between_planes / 2,
                                                         cap_style=CAP_STYLE.square,
                                                         join_style=JOIN_STYLE.mitre,
                                                         resolution=1))
            continue

        # Not a simple overlap - has turns and so on.
        # Use road centreline finding algorithm based on that outlined in
        # https://proceedings.esri.com/library/userconf/proc96/TO400/PAP370/P370.HTM
        if debug:
            print(f"complex overlap")

        # If the overlap polygon is very narrow in places then the default voronoi
        # point density will be too large, and the voronoi edges will not form a complete
        # line that follows the centreline of the overlap polygon. So in that case
        # we try increasing the density a few times
        found_spine = False
        empty_graph = False
        while found_spine is False:
            dense_overlap_part = densify_polygon(overlap_part, voronoi_point_density)
            edges = geoms(ops.voronoi_diagram(dense_overlap_part, edges=True))

            graph = Graph()
            for edge in edges:
                # Remove edges that aren't fully contained or almost-fully contained
                if overlap_part.contains(edge) or (edge.length <= voronoi_point_density and overlap_part.intersects(edge)):
                    node1 = edge.coords[0]
                    node2 = edge.coords[-1]
                    graph.add_node(node1)
                    graph.add_node(node2)
                    graph.add_edge(node1, node2, geom=edge)

            if graph.number_of_nodes() <= 1 or graph.number_of_edges() == 0:
                empty_graph = True
                break

            # Will never finish if there are cycles in the graph - so break them randomly
            for cycle in cycle_basis(graph):
                if debug:
                    print("Breaking cycle...")
                n1 = cycle[0]
                n2 = cycle[1]
                graph.remove_edge(n1, n2)

            # Prune the voronoi edges back until we have a single string of line
            # segments which only has 2 ends (2 nodes with degree=1)
            # TODO a better algorithm here would be to find each leg formed by a deg=3
            #      node, then remove one of the legs completely - though need a robust
            #      way of choosing which leg to remove
            candidate_edges = []
            degrees = dict(graph.degree)
            while max(degrees.values()) > 2:
                for node, degree in list(degrees.items()):
                    if degree == 1:
                        for n1, n2, data in list(graph.edges(node, data=True)):
                            candidate_edges.append(data['geom'])
                            graph.remove_edge(n1, n2)
                            degrees[n1] -= 1
                            degrees[n2] -= 1
                        graph.remove_node(node)

            # Now there should be just 2 nodes with degree=1 (either end of the centre-line)
            deg1_nodes = _nodes_with_degree(graph, 1)
            if deg1_nodes != 2:
                voronoi_point_density /= 2
                if debug:
                    print(f"Initial pruning did not result in a single spine structure, retrying with density {voronoi_point_density}")
                if voronoi_point_density < _VORONOI_DENSITY_MIN:
                    raise ValueError(f"Couldn't find a spine in the overlap, even at voronoi point density {voronoi_point_density}")
                continue

            # Add back in any edges that do not create a fork (so we create a
            # string of line segments which only has 2 ends).
            # candidate_edges is treated as a stack as the edges have to be evaluated in
            # LIFO order so that we build out from the centre-line
            while len(candidate_edges) > 0:
                edge = candidate_edges.pop()
                node1 = edge.coords[0]
                node2 = edge.coords[-1]
                if (node1 in graph or node2 in graph) \
                        and (node1 not in graph or graph.degree(node1) < 2) \
                        and (node2 not in graph or graph.degree(node2) < 2):
                    graph.add_edge(node1, node2, geom=edge)

            deg1_nodes = _nodes_with_degree(graph, 1)
            if deg1_nodes == 2:
                found_spine = True
            else:
                voronoi_point_density /= 2
                if debug:
                    print(f"Re-adding candidate edges resulted in non-single-spine graph, retrying with density {voronoi_point_density}")
                if voronoi_point_density < _VORONOI_DENSITY_MIN:
                    raise ValueError(f"Couldn't find a spine in the overlap, even at voronoi point density {voronoi_point_density}")

        if empty_graph:
            if debug:
                print("No edges inside the overlap, skipping")
            continue

        usable_edges = [e[2].get('geom') for e in graph.edges(data=True)]

        # Find the end points of the line and extend them to touch the closest point
        # of the intersection between the boundaries of p1 and p2
        end_points = []
        for node, degree in list(graph.degree):
            if degree == 1:
                end_points.append(Point(node))

        # There should still be just 2 nodes with degree=1 - should always happen given
        # the voronoi_point_density increases in the while loop above
        assert len(end_points) == 2

        tp = MultiPoint(straight_central_line.coords)
        usable_edges.append(LineString([end_points[0], ops.nearest_points(end_points[0], tp)[1]]))
        usable_edges.append(LineString([end_points[1], ops.nearest_points(end_points[1], tp)[1]]))

        part_splitter = []
        for ls in geoms(ops.linemerge(usable_edges)):
            part_splitter.append(ls.simplify(1.0))
        part_splitter = MultiLineString(part_splitter)
        splitter.append(part_splitter.buffer(min_dist_between_planes / 2,
                                             cap_style=CAP_STYLE.square,
                                             join_style=JOIN_STYLE.mitre,
                                             resolution=1))

    if len(splitter) == 0:
        return p1, p2

    splitter = ops.unary_union(splitter)
    p1_new = largest_polygon(p1.difference(splitter))
    p2_new = largest_polygon(p2.difference(splitter))
    return p1_new, p2_new


def _nodes_with_degree(graph: Graph, degree: int):
    deg_nodes = 0
    for node, ndegree in graph.degree:
        if ndegree == degree:
            deg_nodes += 1

    return deg_nodes
