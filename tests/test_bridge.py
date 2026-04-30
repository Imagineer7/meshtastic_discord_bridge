import meshtastic_discord_bridge as bridge


# ---------------------------------------------------------------------------
# parse_position
# ---------------------------------------------------------------------------

def test_parse_position_float_fields():
    node = {'position': {'latitude': 37.7749, 'longitude': -122.4194, 'altitude': 42}}
    lat, lon, alt = bridge.parse_position(node)
    assert abs(lat - 37.7749) < 1e-6
    assert abs(lon - (-122.4194)) < 1e-6
    assert alt == 42


def test_parse_position_integer_fields():
    node = {'position': {'latitudeI': 377749000, 'longitudeI': -1224194000}}
    lat, lon, alt = bridge.parse_position(node)
    assert abs(lat - 37.7749) < 1e-4
    assert abs(lon - (-122.4194)) < 1e-4
    assert alt is None


def test_parse_position_missing_position_key():
    assert bridge.parse_position({}) == (None, None, None)


def test_parse_position_empty_position_dict():
    assert bridge.parse_position({'position': {}}) == (None, None, None)
