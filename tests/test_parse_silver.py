from commonroad_pipeline.parse_silver import parse_one

# Minimal but valid CommonRoad 2020a XML: one lanelet, one dynamic obstacle
# with a 2-state trajectory, one planning problem.
VALID_XML = """<?xml version="1.0" encoding="UTF-8"?>
<commonRoad commonRoadVersion="2020a" benchmarkID="TEST_001" timeStepSize="0.1"
            author="test" affiliation="test-suite" source="unit-test" date="2026-01-01">
  <location>
    <geoNameId>0</geoNameId>
    <gpsLatitude>0.0</gpsLatitude>
    <gpsLongitude>0.0</gpsLongitude>
  </location>
  <scenarioTags>
  </scenarioTags>
  <lanelet id="100">
    <leftBound>
      <point><x>0.0</x><y>0.0</y></point>
      <point><x>10.0</x><y>0.0</y></point>
    </leftBound>
    <rightBound>
      <point><x>0.0</x><y>3.0</y></point>
      <point><x>10.0</x><y>3.0</y></point>
    </rightBound>
    <lineMarking>solid</lineMarking>
  </lanelet>
  <dynamicObstacle id="200">
    <type>car</type>
    <shape>
      <rectangle>
        <length>5.0</length>
        <width>2.0</width>
      </rectangle>
    </shape>
    <initialState>
      <position>
        <point><x>1.0</x><y>1.5</y></point>
      </position>
      <orientation><exact>0.0</exact></orientation>
      <time><exact>0</exact></time>
      <velocity><exact>5.0</exact></velocity>
      <acceleration><exact>0.0</exact></acceleration>
      <yawRate><exact>0.0</exact></yawRate>
      <slipAngle><exact>0.0</exact></slipAngle>
    </initialState>
    <trajectory>
      <state>
        <position>
          <point><x>1.5</x><y>1.5</y></point>
        </position>
        <orientation><exact>0.0</exact></orientation>
        <time><exact>1</exact></time>
        <velocity><exact>5.0</exact></velocity>
        <acceleration><exact>0.0</exact></acceleration>
      </state>
      <state>
        <position>
          <point><x>2.0</x><y>1.5</y></point>
        </position>
        <orientation><exact>0.0</exact></orientation>
        <time><exact>2</exact></time>
        <velocity><exact>5.0</exact></velocity>
        <acceleration><exact>0.0</exact></acceleration>
      </state>
    </trajectory>
  </dynamicObstacle>
  <planningProblem id="300">
    <initialState>
      <position>
        <point><x>0.0</x><y>1.5</y></point>
      </position>
      <orientation><exact>0.0</exact></orientation>
      <time><exact>0</exact></time>
      <velocity><exact>4.0</exact></velocity>
      <acceleration><exact>0.0</exact></acceleration>
      <yawRate><exact>0.0</exact></yawRate>
      <slipAngle><exact>0.0</exact></slipAngle>
    </initialState>
    <goalState>
      <position>
        <rectangle>
          <length>2.0</length>
          <width>2.0</width>
          <center><x>9.0</x><y>1.5</y></center>
        </rectangle>
      </position>
      <time><intervalStart>5</intervalStart><intervalEnd>10</intervalEnd></time>
    </goalState>
  </planningProblem>
</commonRoad>
"""

TRUNCATED_XML = '<?xml version="1.0" enco'


def test_parse_one_valid_scenario():
    (scenario_row, lanelet_rows, obstacle_rows,
     obstacle_state_rows, planning_problem_rows, failure_row) = parse_one(
        "TEST_001", "test_001.xml", VALID_XML
    )
    assert failure_row == {}

    assert scenario_row["num_lanelets"] == 1
    assert scenario_row["num_obstacles"] == 1
    assert scenario_row["num_dynamic_obstacles"] == 1
    assert scenario_row["num_planning_problems"] == 1

    assert len(lanelet_rows) == 1
    assert lanelet_rows[0]["lanelet_id"] == 100

    assert len(obstacle_rows) == 1
    assert obstacle_rows[0]["obstacle_role"] == "dynamic"
    assert obstacle_rows[0]["shape_length"] == 5.0

    # trajectory has 2 states -> 2 obstacle_states rows
    assert len(obstacle_state_rows) == 2
    assert obstacle_state_rows[0]["time_step"] == 1
    assert obstacle_state_rows[0]["x"] == 1.5

    assert len(planning_problem_rows) == 1
    assert planning_problem_rows[0]["planning_problem_id"] == 300
    assert planning_problem_rows[0]["goal_time_step_start"] == 5.0
    assert planning_problem_rows[0]["goal_time_step_end"] == 10.0


def test_parse_one_truncated_xml_reports_failure_not_exception():
    (scenario_row, lanelet_rows, obstacle_rows,
     obstacle_state_rows, planning_problem_rows, failure_row) = parse_one(
        "BAD_001", "bad_001.xml", TRUNCATED_XML
    )
    assert scenario_row == {}
    assert lanelet_rows == []
    assert obstacle_rows == []
    assert obstacle_state_rows == []
    assert planning_problem_rows == []
    assert failure_row["file_name"] == "bad_001.xml"
    assert "error" in failure_row
