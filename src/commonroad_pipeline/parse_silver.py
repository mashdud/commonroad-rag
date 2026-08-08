"""
Task 2: bronze -> silver parsing with commonroad-io.

Runs driver-side (commonroad-io scenario objects aren't Spark-serializable).
Suitable for up to a few thousand files per run on a reasonably sized
driver; beyond that, switch to mapInPandas with per-partition temp files.

Every row either lands in the structured silver tables, or in
parse_failures with the exact exception message -- nothing is silently
dropped.

Run as a Databricks Job task:
    python -m commonroad_pipeline.parse_silver
"""

import logging
import tempfile
from typing import Any, Dict, List, Tuple

from pyspark.sql import SparkSession

from commonroad_pipeline.config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("parse_silver")


def _safe_float(val) -> float:
    return float(val) if val is not None else None


def _state_to_row(scenario_id: str, obstacle_id: int, state) -> Dict[str, Any]:
    """Flatten a commonroad State/ExtendedPMState into a flat dict.
    Not all state types carry all fields (e.g. trajectory states lack
    yaw_rate/slip_angle), so every field is fetched with getattr(default=None)."""
    position = getattr(state, "position", None)
    x, y = (float(position[0]), float(position[1])) if position is not None else (None, None)
    return {
        "scenario_id": scenario_id,
        "obstacle_id": obstacle_id,
        "time_step": int(state.time_step),
        "x": x,
        "y": y,
        "orientation": _safe_float(getattr(state, "orientation", None)),
        "velocity": _safe_float(getattr(state, "velocity", None)),
        "acceleration": _safe_float(getattr(state, "acceleration", None)),
    }


def _extract_goal_time_step(goal) -> Tuple[Any, Any]:
    """Goal region state_list entries carry a time_step Interval. Extract
    start/end conservatively -- goal geometry (position/velocity intervals)
    is intentionally not flattened here; irregular shapes across scenarios
    make that a separate table for a later pass."""
    try:
        state = goal.state_list[0]
        interval = state.time_step
        return float(interval.start), float(interval.end)
    except (IndexError, AttributeError):
        return None, None


def parse_one(scenario_id: str, file_name: str, xml_content: str) -> Tuple[
    Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]],
    List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]
]:
    """Parse a single scenario's XML content.
    Returns (scenario_row, lanelet_rows, obstacle_rows, obstacle_state_rows,
    planning_problem_rows, failure_row). Exactly one of (scenario_row,
    failure_row) is populated."""
    from commonroad.common.file_reader import CommonRoadFileReader

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False)
    try:
        tmp.write(xml_content)
        tmp.close()

        scenario, planning_problem_set = CommonRoadFileReader(tmp.name).open()
        file_info = scenario.lanelet_network.meta_information.file_information
        location = scenario.lanelet_network.location

        scenario_row = {
            "scenario_id": str(scenario.scenario_id),
            "file_name": file_name,
            "dt": float(scenario.dt),
            "author": file_info.author,
            "affiliation": file_info.affiliation,
            "source": file_info.source,
            "tags": [str(t) for t in scenario.tags] if scenario.tags else [],
            "geo_name_id": int(location.geo_name_id) if location.geo_name_id is not None else None,
            "gps_latitude": _safe_float(location.gps_latitude),
            "gps_longitude": _safe_float(location.gps_longitude),
            "num_lanelets": len(scenario.lanelet_network.lanelets),
            "num_obstacles": len(scenario.obstacles),
            "num_dynamic_obstacles": len(scenario.dynamic_obstacles),
            "num_static_obstacles": len(scenario.static_obstacles),
            "num_planning_problems": len(planning_problem_set.planning_problem_dict),
        }

        lanelet_rows = [
            {
                "scenario_id": str(scenario.scenario_id),
                "lanelet_id": int(lanelet.lanelet_id),
                "predecessor": [int(p) for p in lanelet.predecessor],
                "successor": [int(s) for s in lanelet.successor],
                "adj_left": int(lanelet.adj_left) if lanelet.adj_left is not None else None,
                "adj_right": int(lanelet.adj_right) if lanelet.adj_right is not None else None,
                "num_center_vertices": len(lanelet.center_vertices),
            }
            for lanelet in scenario.lanelet_network.lanelets
        ]

        obstacle_rows = []
        obstacle_state_rows = []
        dynamic_ids = {o.obstacle_id for o in scenario.dynamic_obstacles}

        for obstacle in scenario.obstacles:
            shape = obstacle.obstacle_shape
            init = obstacle.initial_state
            init_position = getattr(init, "position", None)

            obstacle_rows.append({
                "scenario_id": str(scenario.scenario_id),
                "obstacle_id": int(obstacle.obstacle_id),
                "obstacle_type": str(obstacle.obstacle_type.value),
                "obstacle_role": "dynamic" if obstacle.obstacle_id in dynamic_ids else "static",
                "shape_type": type(shape).__name__,
                "shape_length": _safe_float(getattr(shape, "length", None)),
                "shape_width": _safe_float(getattr(shape, "width", None)),
                "initial_x": float(init_position[0]) if init_position is not None else None,
                "initial_y": float(init_position[1]) if init_position is not None else None,
                "initial_orientation": _safe_float(getattr(init, "orientation", None)),
                "initial_velocity": _safe_float(getattr(init, "velocity", None)),
                "initial_acceleration": _safe_float(getattr(init, "acceleration", None)),
                "initial_time_step": int(init.time_step),
            })

            # Trajectory states -- only dynamic obstacles carry a prediction
            prediction = getattr(obstacle, "prediction", None)
            trajectory = getattr(prediction, "trajectory", None)
            if trajectory is not None:
                obstacle_state_rows.extend(
                    _state_to_row(str(scenario.scenario_id), int(obstacle.obstacle_id), s)
                    for s in trajectory.state_list
                )

        planning_problem_rows = []
        for pp_id, pp in planning_problem_set.planning_problem_dict.items():
            init = pp.initial_state
            init_position = getattr(init, "position", None)
            goal_start, goal_end = _extract_goal_time_step(pp.goal)

            planning_problem_rows.append({
                "scenario_id": str(scenario.scenario_id),
                "planning_problem_id": int(pp_id),
                "initial_x": float(init_position[0]) if init_position is not None else None,
                "initial_y": float(init_position[1]) if init_position is not None else None,
                "initial_orientation": _safe_float(getattr(init, "orientation", None)),
                "initial_velocity": _safe_float(getattr(init, "velocity", None)),
                "initial_acceleration": _safe_float(getattr(init, "acceleration", None)),
                "initial_time_step": int(init.time_step),
                "goal_time_step_start": goal_start,
                "goal_time_step_end": goal_end,
            })

        return scenario_row, lanelet_rows, obstacle_rows, obstacle_state_rows, planning_problem_rows, {}

    except Exception as e:
        failure_row = {"scenario_id": scenario_id, "file_name": file_name, "error": str(e)}
        return {}, [], [], [], [], failure_row


def run(spark: SparkSession) -> None:
    cfg = load_config()
    bronze_df = spark.table(cfg.bronze_table)
    rows = bronze_df.select("scenario_id", "file_name", "xml_content").collect()
    logger.info("Parsing %d bronze rows", len(rows))

    scenario_records, lanelet_records, obstacle_records = [], [], []
    obstacle_state_records, planning_problem_records, failed_records = [], [], []

    for row in rows:
        scenario_row, lanelet_rows, obstacle_rows, state_rows, pp_rows, failure_row = parse_one(
            row["scenario_id"], row["file_name"], row["xml_content"]
        )
        if scenario_row:
            scenario_records.append(scenario_row)
            lanelet_records.extend(lanelet_rows)
            obstacle_records.extend(obstacle_rows)
            obstacle_state_records.extend(state_rows)
            planning_problem_records.extend(pp_rows)
        if failure_row:
            failed_records.append(failure_row)

    logger.info("Parsed: %d | Failed: %d", len(scenario_records), len(failed_records))

    table_map = {
        cfg.silver_scenarios_table: scenario_records,
        cfg.silver_lanelets_table: lanelet_records,
        cfg.silver_obstacles_table: obstacle_records,
        cfg.silver_obstacle_states_table: obstacle_state_records,
        cfg.silver_planning_problems_table: planning_problem_records,
    }
    for table_name, records in table_map.items():
        if records:
            spark.createDataFrame(records).write.mode("overwrite").saveAsTable(table_name)

    if failed_records:
        spark.createDataFrame(failed_records).write.mode("overwrite").saveAsTable(cfg.silver_failed_table)

    if len(rows) > 0 and len(failed_records) / len(rows) > 0.5:
        raise RuntimeError(
            f"Parse failure rate {len(failed_records)}/{len(rows)} exceeds 50% "
            f"threshold -- failing job so it doesn't silently propagate bad silver data."
        )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    run(spark)


if __name__ == "__main__":
    main()
